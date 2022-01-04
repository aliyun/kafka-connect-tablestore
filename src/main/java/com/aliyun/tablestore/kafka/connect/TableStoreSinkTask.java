package com.aliyun.tablestore.kafka.connect;

import com.aliyun.tablestore.kafka.connect.enums.RunTimeErrorMode;
import com.aliyun.tablestore.kafka.connect.enums.TablestoreMode;
import com.aliyun.tablestore.kafka.connect.errors.ErrorReporter;
import com.aliyun.tablestore.kafka.connect.errors.KafkaReporter;
import com.aliyun.tablestore.kafka.connect.errors.TableStoreReporter;
import com.aliyun.tablestore.kafka.connect.model.ErrantSinkRecord;
import com.aliyun.tablestore.kafka.connect.utils.Version;
import com.aliyun.tablestore.kafka.connect.writer.TableStoreSinkWriterInterface;
import com.aliyun.tablestore.kafka.connect.writer.TimeseriesSinkWriter;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


public class TableStoreSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreSinkTask.class);

    private TableStoreSinkConfig config;
    private TableStoreSinkWriterInterface tableStoreSinkWriter;
    private ErrorReporter errorReporter;

    public TableStoreSinkTask() {
    }

    /**
     * 获取 Connector的版本
     *
     * @return Connector的版本号
     */
    @Override
    public String version() {
        return Version.getVersion();
    }

    /**
     * 任务初始化
     *
     * @param properties
     */
    @Override
    public void start(Map<String, String> properties) {
        LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter START");
        config = new TableStoreSinkConfig(properties);
        if (RunTimeErrorMode.KAFKA.equals(config.getRunTimeErrorMode())) {
            errorReporter = new KafkaReporter(config);
        } else if (RunTimeErrorMode.TABLESTORE.equals(config.getRunTimeErrorMode())){
            errorReporter = new TableStoreReporter(config);
        }

    }

    /**
     * 分区分配
     *
     * @param partitions
     */
    @Override
    public void open(Collection<TopicPartition> partitions) {
        LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter OPEN");
        tableStoreSinkWriter = getSinkWriterByConfigMode(config);

        Set<String> topics = new HashSet<>();
        for (TopicPartition partition : partitions) {
            LOGGER.info("Thread(" + Thread.currentThread().getId() + ") OPEN (topic: " +
                    partition.topic() + ", partition: " + partition.partition() + ")");
            topics.add(partition.topic());
        }
        tableStoreSinkWriter.initWriter(topics);
    }

    /**
     * 读取 SinkRecord，进行数据处理后写入 OTS 表
     *
     * @param records
     */
    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        // 这里判断是否过期
        if (tableStoreSinkWriter.needRebuild()) {
            tableStoreSinkWriter.rebuildClient();
        }

        final int recordsCount = records.size();
        LOGGER.debug("Received {} records. Writing them to the TableStore", recordsCount);

        List<ErrantSinkRecord> errantRecords = tableStoreSinkWriter.write(records);
        if(errorReporter!=null){
            errorReporter.report(errantRecords);
        }
    }



    /**
     * 分区关闭
     *
     * @param partitions
     */
    @Override
    public void close(Collection<TopicPartition> partitions) {
        LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter Close");
        for (TopicPartition partition : partitions) {
            LOGGER.info("Thread(" + Thread.currentThread().getId() + ") CLOSE (topic: " +
                    partition.topic() + ", partition: " + partition.partition() + ")");
        }
        tableStoreSinkWriter.closeWriters();
    }


    /**
     * 停止任务
     */
    @Override
    public void stop() {
        LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter Stop");

        tableStoreSinkWriter.close();
        if(errorReporter!=null){
            errorReporter.close();
        }
    }



    private TableStoreSinkWriterInterface getSinkWriterByConfigMode(TableStoreSinkConfig config) {
        TablestoreMode mode = config.getTablestoreMode();
        switch(mode) {
            case NORMAL:
                return new TableStoreSinkWriter(config);
            case TIMESERIES:
                return new TimeseriesSinkWriter(config);
            default:
                LOGGER.error(String.format("Error while init sink writer, mode string: %s", mode));
                return null;
        }
    }

}
