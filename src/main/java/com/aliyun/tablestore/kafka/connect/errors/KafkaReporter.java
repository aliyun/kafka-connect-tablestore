package com.aliyun.tablestore.kafka.connect.errors;

import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Properties;

/**
 * Kafka错误报告器：需要指定 Kafka集群和 topic
 * 在报告的Kafka消息 Header中增加 ErrorInfo 字段; Key 和 Value 不变
 */
public class KafkaReporter extends GenericErrorReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaReporter.class);

    private final TableStoreSinkConfig config;
    private Properties props;
    private String topic;
    private KafkaProducer<Byte[], Byte[]> producer;


    public KafkaReporter(TableStoreSinkConfig config) {
        super();
        LOGGER.info("Initialize Kafka Error Reporter");
        this.config = config;
        topic = config.getString(TableStoreSinkConfig.RUNTIME_ERROR_TOPIC_NAME);
        initProducer();
    }

    /**
     * 初始化 Kafka 生产者
     */
    private void initProducer() {
        props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(TableStoreSinkConfig.RUNTIME_ERROR_BOOTSTRAP_SERVERS));
        //Kafka消息的序列化方式,默认ByteArray
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        //请求的最长等待时间
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5 * 1000);

        producer = new KafkaProducer<>(props);
    }

    @Override
    public String className() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void report(SinkRecord errantRecord, Throwable error) {
        String errorInfo = convertError(error);

        ProducerRecord<Byte[], Byte[]> producerRecord = convertToProducerRecord(errantRecord, errorInfo);

        this.producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                LOGGER.error("Could not produce message to runtime error queue. topic=" + topic, exception);
            }
            if (metadata == null) {
                LOGGER.error("Failed to write message to runtime error queue.");
            } else {
                LOGGER.info("Succeed to write message to runtime error queue:" + metadata.toString());
            }
        });

    }

    /**
     * 生成 ProducerRecord，键值都为 byte[]
     *
     * @param errantRecord 原记录
     * @param errorInfo    错误信息
     */
    private ProducerRecord<Byte[], Byte[]> convertToProducerRecord(SinkRecord errantRecord, String errorInfo) {
        if (errantRecord == null) {
            return null;
        }

        byte[] key = this.keyConverter.fromConnectData(topic, errantRecord.keySchema(), errantRecord.key());
        byte[] value = this.valueConverter.fromConnectData(topic, errantRecord.valueSchema(), errantRecord.value());

        ProducerRecord<Byte[], Byte[]> producerRecord = new ProducerRecord(
                topic, null, checkAndConvertTimestamp(errantRecord.timestamp()), key, value
        );

        Headers headers = errantRecord.headers();
        if (headers != null) {
            Iterator iterator = headers.iterator();

            while (iterator.hasNext()) {
                Header header = (Header) iterator.next();
                String headerKey = header.key();
                byte[] rawHeader = this.headerConverter.fromConnectHeader(topic, headerKey, header.schema(), header.value());
                producerRecord.headers().add(headerKey, rawHeader);
            }
        }

        producerRecord.headers().add("ErrorInfo", errorInfo.getBytes());

        return producerRecord;
    }

    @Override
    public void flush() {
        this.producer.flush();
    }

    @Override
    public void close() {
        LOGGER.info("Shutdown Kafka Error Reporter");
        this.producer.flush();
        this.producer.close();
    }
}
