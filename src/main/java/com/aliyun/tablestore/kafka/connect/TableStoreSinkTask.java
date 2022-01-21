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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


public class TableStoreSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreSinkTask.class);

    private TableStoreSinkConfig config;
    private TableStoreSinkWriterInterface tableStoreSinkWriter;
    private ErrorReporter errorReporter;

    private static Map<String, TableStoreSinkWriterInterface> WRITERINTERFACE_MAP = new ConcurrentHashMap<>();
    private static Map<String, ErrorReporter> ERRORREPORT_MAP = new ConcurrentHashMap<>();
    private static Map<String, AtomicInteger> COUNT_MAP = new ConcurrentHashMap<>();


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
        String name = config.getName();
        synchronized (TableStoreSinkTask.class) {
            AtomicInteger count = COUNT_MAP.computeIfAbsent(name, e -> new AtomicInteger(0));
            count.incrementAndGet();

            LOGGER.info(String.format("init errorReport. connectorName: %s", name));

            errorReporter = ERRORREPORT_MAP.computeIfAbsent(name, e -> createErrorReport(config));
        }
    }


    private ErrorReporter createErrorReport(TableStoreSinkConfig config) {
        if (RunTimeErrorMode.KAFKA.equals(config.getRunTimeErrorMode())) {
            return new KafkaReporter(config);
        } else if (RunTimeErrorMode.TABLESTORE.equals(config.getRunTimeErrorMode())) {
            return new TableStoreReporter(config);
        } else {
            return null;
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
        Set<String> topics = new HashSet<>();
        for (TopicPartition partition : partitions) {
            LOGGER.info("Thread(" + Thread.currentThread().getId() + ") OPEN (topic: " +
                    partition.topic() + ", partition: " + partition.partition() + ")");
            topics.add(partition.topic());
        }
        synchronized (TableStoreSinkTask.class) {
            LOGGER.info(String.format("init tableStoreSinkWriter. connectorName:%s", config.getName()));
            tableStoreSinkWriter = WRITERINTERFACE_MAP.computeIfAbsent(config.getName(), e -> getSinkWriterByConfigMode(config));
            tableStoreSinkWriter.initWriter(topics);
        }
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
    }


    /**
     * 停止任务
     */
    @Override
    public void stop() {

        LOGGER.info("Thread(" + Thread.currentThread().getId() + ") Enter Stop");
        String connectorName = config.getName();
        AtomicInteger atomic = COUNT_MAP.get(connectorName);
        int left = 0;
        synchronized (TableStoreSinkTask.class) {
            if (atomic != null) {
                left = atomic.decrementAndGet();
            }
            LOGGER.info(String.format("num of left task is %s, connector name : %s", left, connectorName));

            if (left <= 0) {
                LOGGER.info(String.format("no task left, release resource. connectorName : %s", connectorName));
                tableStoreSinkWriter.closeWriters();
                tableStoreSinkWriter.close();
                if (errorReporter != null) {
                    errorReporter.close();
                }

                WRITERINTERFACE_MAP.remove(connectorName);
                ERRORREPORT_MAP.remove(connectorName);
                COUNT_MAP.remove(connectorName);
            }
        }

        errorReporter = null;
        tableStoreSinkWriter = null;
        config = null;
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
