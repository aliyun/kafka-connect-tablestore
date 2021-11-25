package com.aliyun.tablestore.kafka.connect;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TableStoreSinkConfigTest {
    TableStoreSinkConfig config;
    Map<String, String> props;
    String topics="test";

    @Before
    public void before(){
        props = new HashMap<>();

        props.put(TableStoreSinkConfig.TOPIC_LIST, topics);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
//        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
//        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);
    }
    @Test
    public void testTableNameByTopic() {
        //根据 table.name.format 映射表
        props.put(TableStoreSinkConfig.TABLE_NAME_FORMAT,"<topic>_kafka");
        config=new TableStoreSinkConfig(props);
        Assert.assertEquals("test_kafka",config.getTableNameByTopic("test"));
        Assert.assertEquals("test_kafka",config.getTableNameList().get(0));

        //根据 topics.assign.tables 映射表
        props.put(TableStoreSinkConfig.TOPIC_ASSIGN_TABLE,"test:destTable");
        config=new TableStoreSinkConfig(props);
        Assert.assertEquals("destTable",config.getTableNameByTopic("test"));
        Assert.assertEquals("destTable",config.getTableNameList().get(0));
    }
}