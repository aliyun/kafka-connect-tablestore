package com.aliyun.tablestore.kafka.connect;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class KafkaErrorReporterTest {
    String TOPIC = "test_error_reporter";
    String ERROR_TOPIC = "test_error";
    int PARTITION = 0;

    TableStoreSinkTask task;
    Map<String, String> props;


    KafkaConsumer consumer;
    KafkaConsumerRunner kafkaConsumerRunner;
    ConcurrentLinkedQueue<ConsumerRecord<Byte[], Byte[]>> queue;

    @Before
    public void before() throws InterruptedException {
        props = new HashMap<>();

        props.put(TableStoreSinkConfig.TOPIC_LIST, TOPIC);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);
        props.put(TableStoreSinkConfig.AUTO_CREATE, "true");
        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "kafka");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_TOLERANCE, "all");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_MODE, "kafka");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_BOOTSTRAP_SERVERS, "localhost:9092");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_TOPIC_NAME, ERROR_TOPIC);

        task = new TableStoreSinkTask();

        task.start(props);
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
        task.open(Collections.singletonList(topicPartition));

        queue = new ConcurrentLinkedQueue();
        consumer = createConsumer();
        kafkaConsumerRunner = new KafkaConsumerRunner(consumer);
        Thread consumerThread = new Thread(kafkaConsumerRunner);
        consumerThread.start();
    }


    @After
    public void after() {
        task.stop();
        kafkaConsumerRunner.shutdown();
    }

    @Test
    public void KafkaReporterTest() throws InterruptedException {
        Long timestamp = System.currentTimeMillis();
        System.out.println("TimeStamp:" + timestamp);
        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, Schema.INT32_SCHEMA, 1, Schema.STRING_SCHEMA, "test", 1L, timestamp, TimestampType.NO_TIMESTAMP_TYPE);
        task.put(Collections.singletonList(sinkRecord));


        ConsumerRecord consumerRecord = null;
        while (consumerRecord == null || consumerRecord.timestamp() != timestamp) {
            if (queue.isEmpty()) {
                Thread.sleep(10);
            } else {
                consumerRecord = queue.poll();
            }
        }
        Headers headers = consumerRecord.headers();
        Iterator<Header> iterator = headers.iterator();
        while (iterator.hasNext()) {
            Header header = iterator.next();
            System.out.println(header.key());
            System.out.println(new String(header.value()));
        }

    }

    private KafkaConsumer createConsumer() {
        // create instance for properties to access producer configs
        Properties comsumerProps = new Properties();

        //Assign localhost id
        comsumerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        //Set GroupID for consumer.
        comsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        //If it is true, the consumer will auto-commit for the offset,
        comsumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        //The time interval of the updated consumption offset written to ZooKeeper
        comsumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        //Time to wait before giving up or continuing to consume messages
        comsumerProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

        //key serialization
        comsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        //value serialization
        comsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        //从最新开始消费
        comsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        KafkaConsumer consumer = new KafkaConsumer(comsumerProps);
        consumer.subscribe(Arrays.asList(ERROR_TOPIC));

        return consumer;
    }

    class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer consumer;

        KafkaConsumerRunner(KafkaConsumer consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                while (!closed.get()) {
                    try {
                        ConsumerRecords<Byte[], Byte[]> records = consumer.poll(1000);
                        //必须在下次Poll之前消费完这些数据, 且总耗时不得超过SESSION_TIMEOUT_MS_CONFIG。
                        for (ConsumerRecord<Byte[], Byte[]> record : records) {
                            queue.add(record);
                            System.out.println(String.format("Thread:%s Consume partition:%d offset:%d,timestamp:%d", Thread.currentThread().getName(), record.partition(), record.offset(), record.timestamp()));
                        }
                    } catch (Exception e) {
                        try {
                            Thread.sleep(1000);
                        } catch (Throwable ignore) {

                        }
                        e.printStackTrace();
                    }
                }
            } catch (WakeupException e) {
                //如果关闭则忽略异常。
                if (!closed.get()) {
                    throw e;
                }
            } finally {
                consumer.commitAsync();
                consumer.close();
            }
        }

        //可以被另一个线程调用的关闭Hook。
        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }

}


