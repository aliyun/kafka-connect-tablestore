package com.aliyun.tablestore.kafka.connect;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class TableStoreWriterTest {
    private final String TOPIC = "test_writer";
    private final int PARTITION = 0;

    private Map<String, String> props;
    private TableStoreSinkConfig config;
    TableStoreSinkWriter writer;
    private static SyncClient client;

    private static final Schema KEY_SCHEMA;
    private static final Schema VALUE_SCHEMA;

    static {
        KEY_SCHEMA = SchemaBuilder.struct()
                .field("pk0", Schema.STRING_SCHEMA)
                .build();

        VALUE_SCHEMA = SchemaBuilder.struct()
                .field("A", Schema.OPTIONAL_STRING_SCHEMA)
                .field("B", Schema.OPTIONAL_INT32_SCHEMA)
                .field("C", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    @Before
    public void before() {
        props = new HashMap<>();

        props.put(TableStoreSinkConfig.TOPIC_LIST, TOPIC);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);
        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "record_key");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "pk0");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "string");
        props.put(TableStoreSinkConfig.AUTO_CREATE, "true");

        config = new TableStoreSinkConfig(props);

        writer = new TableStoreSinkWriter(config);


        client = new SyncClient(AccessKey.endpoint, AccessKey.accessKeyId, AccessKey.accessKeySecret, AccessKey.instanceName);
        client.deleteTable(new DeleteTableRequest(TOPIC));
    }

    @After
    public void after() {
        writer.closeWriters();
        writer.close();
    }


    @Test
    public void testWriter() {
        writer.initWriter(TOPIC);

        TableMeta tableMeta = describeTable(TOPIC);

        List<PrimaryKeySchema> pkDefinedInMeta = tableMeta.getPrimaryKeyList();
        List<PrimaryKeySchema> pkDefinedInConfig = config.getPrimaryKeySchemaListByTable(TOPIC);

        Assert.assertEquals(pkDefinedInConfig.size(), pkDefinedInMeta.size());

        for (int i = 0; i < pkDefinedInConfig.size(); ++i) {
            PrimaryKeySchema pkSchemaInMeta = pkDefinedInMeta.get(i);
            PrimaryKeySchema pkSchemaInConfig = pkDefinedInConfig.get(i);

            Assert.assertEquals(pkSchemaInMeta.getName(), pkSchemaInConfig.getName());
            Assert.assertEquals(pkSchemaInMeta.getType(), pkSchemaInConfig.getType());
            Assert.assertEquals(pkSchemaInConfig.getOption(), pkSchemaInMeta.getOption());
        }

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "pk_value");

        List<SinkRecord> list = new ArrayList<SinkRecord>();
        for (int i = 0; i < 1000; i++) {
            Struct value = new Struct(VALUE_SCHEMA)
                    .put("A", String.valueOf(i));
            SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);
            list.add(sinkRecord);
        }
        writer.write(list);
        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put("pk0", PrimaryKeyValue.fromString("pk_value"));
        Row row = getRow(primaryKeyMap);
        Assert.assertEquals("999", row.getColumn("A").get(0).getValue().asString());
    }

    public TableMeta describeTable(String tableName) {
        DescribeTableResponse response = client.describeTable(new DescribeTableRequest(tableName));
        return response.getTableMeta();
    }

    public Row getRow(Map<String, PrimaryKeyValue> primaryKeyMap) {
        //构造主键。
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (Map.Entry<String, PrimaryKeyValue> entry : primaryKeyMap.entrySet()) {
            primaryKeyBuilder.addPrimaryKeyColumn(entry.getKey(), entry.getValue());
        }
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        //读取一行数据，设置数据表名称。
        SingleRowQueryCriteria criteria = new SingleRowQueryCriteria(TOPIC, primaryKey);
        //设置读取最新版本。
        criteria.setMaxVersions(1);
        GetRowResponse getRowResponse = client.getRow(new GetRowRequest(criteria));
        Row row = getRowResponse.getRow();

        return row;
    }
}
