package com.aliyun.tablestore.kafka.connect;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TableStoreErrorReporterTest {
    String TOPIC = "test_error_reporter";
    String ERROR_TABLE_NAME = "test_error";
    int PARTITION = 0;

    TableStoreSinkTask task;
    Map<String, String> props;
    SyncClient client;

    @Before
    public void before() {
        client = new SyncClient(AccessKey.endpoint, AccessKey.accessKeyId, AccessKey.accessKeySecret, AccessKey.instanceName);
        try {
            client.deleteTable(new DeleteTableRequest(ERROR_TABLE_NAME));
        } catch (Exception ignore) {

        }

        props = new HashMap<>();

        props.put(TableStoreSinkConfig.TOPIC_LIST, TOPIC);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
//        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
//        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);
        props.put(TableStoreSinkConfig.AUTO_CREATE, "true");
        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "kafka");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_TOLERANCE, "all");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_MODE, "tablestore");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_TABLE_NAME, ERROR_TABLE_NAME);

        task = new TableStoreSinkTask();

        task.start(props);
        TopicPartition topicPartition = new TopicPartition(TOPIC, PARTITION);
        task.open(Collections.singletonList(topicPartition));
    }


    @After
    public void after() {
        task.stop();
        client.shutdown();
    }

    @Test
    public void TableStoreReporterTest() {
        long offset = 1L;
        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, Schema.STRING_SCHEMA, "test", offset);
        task.put(Collections.singletonList(sinkRecord));

        Row row = getRow(offset);

        Assert.assertTrue(row != null);
    }


    public Row getRow(long offset) {
        //构造主键。
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(TableStoreSinkConfig.PRIMARY_KEY_NAME_TOPIC_PARTITION, PrimaryKeyValue.fromString(TOPIC + "_" + PARTITION))
                .addPrimaryKeyColumn(TableStoreSinkConfig.PRIMARY_KEY_NAME_OFFSET, PrimaryKeyValue.fromLong(offset));
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        //读取一行数据，设置数据表名称。
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(ERROR_TABLE_NAME, primaryKey);
        //设置读取最新版本。
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        return row;
    }
}
