package functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.AccessKey;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class StructSchemaTest {
    String TOPIC = "test_struct";
    int PARTITION = 0;
    TopicPartition topicPartition;
    Map<String, String> props;
    SyncClient client;
    TableStoreSinkTask task;
    JsonConverter jsonConverter;

    private static final Schema VALUE_SCHEMA;

    static {
        VALUE_SCHEMA = SchemaBuilder.struct()
                .field("string", Schema.STRING_SCHEMA)
                .field("byte", Schema.INT8_SCHEMA)
                .field("short", Schema.INT16_SCHEMA)
                .field("int", Schema.INT32_SCHEMA)
                .field("long", Schema.INT64_SCHEMA)
                .field("float", Schema.FLOAT32_SCHEMA)
                .field("double", Schema.FLOAT64_SCHEMA)
                .field("boolean", Schema.BOOLEAN_SCHEMA)
                .field("bytes", Schema.BYTES_SCHEMA)
                .build();

    }

    @Before
    public void before() {
        topicPartition = new TopicPartition(TOPIC, PARTITION);

        props = new HashMap<>();
        props.put("name", "kafka-tablestore-sink");
        props.put("connector.class", "TableStoreSinkConnector");
        props.put("task.max", "1");
        props.put(TableStoreSinkConfig.TOPIC_LIST, TOPIC);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);
        props.put(TableStoreSinkConfig.AUTO_CREATE, "true");

        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "record_value");

        task = new TableStoreSinkTask();

        //JsonConverter
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");

        jsonConverter = new JsonConverter();
        jsonConverter.configure(jsonProps);


        client = new SyncClient(AccessKey.endpoint, AccessKey.accessKeyId, AccessKey.accessKeySecret, AccessKey.instanceName);

    }

    @After
    public void after() {
        client.shutdown();
        task.stop();
    }

    @Test
    public void testStructSchemaStringPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "string";
        String pkValue = "test";

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "string");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", pkValue)
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", pkValue.getBytes());


        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromString(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals((byte) 1, columnsMap.get("byte").firstEntry().getValue().asLong());
        Assert.assertEquals((short) 1, columnsMap.get("short").firstEntry().getValue().asLong());
        Assert.assertEquals(1, columnsMap.get("int").firstEntry().getValue().asLong());
        Assert.assertEquals(1L, columnsMap.get("long").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());
        Assert.assertEquals("test", new String(columnsMap.get("bytes").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testStructSchemaBytePk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }

        String pkName = "byte";
        byte pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "integer");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", pkValue)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromLong((pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", columnsMap.get("string").firstEntry().getValue().asString());
        Assert.assertEquals((short) 1, columnsMap.get("short").firstEntry().getValue().asLong());
        Assert.assertEquals(1, columnsMap.get("int").firstEntry().getValue().asLong());
        Assert.assertEquals(1L, columnsMap.get("long").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());
        Assert.assertEquals("test", new String(columnsMap.get("bytes").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testStructSchemaShortPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "short";
        short pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "integer");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", pkValue)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());


        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromLong(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", columnsMap.get("string").firstEntry().getValue().asString());
        Assert.assertEquals((byte) 1, columnsMap.get("byte").firstEntry().getValue().asLong());
        Assert.assertEquals(1, columnsMap.get("int").firstEntry().getValue().asLong());
        Assert.assertEquals(1L, columnsMap.get("long").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());
        Assert.assertEquals("test", new String(columnsMap.get("bytes").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testStructSchemaIntPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "int";
        int pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "integer");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", pkValue)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromLong(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", columnsMap.get("string").firstEntry().getValue().asString());
        Assert.assertEquals((byte) 1, columnsMap.get("byte").firstEntry().getValue().asLong());
        Assert.assertEquals((short) 1, columnsMap.get("short").firstEntry().getValue().asLong());
        Assert.assertEquals(1L, columnsMap.get("long").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());
        Assert.assertEquals("test", new String(columnsMap.get("bytes").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testStructSchemaLongPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "long";
        long pkValue = 1L;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "integer");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", pkValue)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromLong(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", columnsMap.get("string").firstEntry().getValue().asString());
        Assert.assertEquals((byte) 1, columnsMap.get("byte").firstEntry().getValue().asLong());
        Assert.assertEquals((short) 1, columnsMap.get("short").firstEntry().getValue().asLong());
        Assert.assertEquals(1, columnsMap.get("int").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());
        Assert.assertEquals("test", new String(columnsMap.get("bytes").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testStructSchemaBytesPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "bytes";
        byte[] pkValue = "test".getBytes();

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", pkValue);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", columnsMap.get("string").firstEntry().getValue().asString());
        Assert.assertEquals((byte) 1, columnsMap.get("byte").firstEntry().getValue().asLong());
        Assert.assertEquals((short) 1, columnsMap.get("short").firstEntry().getValue().asLong());
        Assert.assertEquals(1, columnsMap.get("int").firstEntry().getValue().asLong());
        Assert.assertEquals(1L, columnsMap.get("long").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());
    }

    @Test
    public void testStructSchemaMultiPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "string,int,bytes");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "string,integer,binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", (byte) 1)
                .put("short", (short) 1)
                .put("int", 1)
                .put("long", 1L)
                .put("float", 1.0f)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());


        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put("string", PrimaryKeyValue.fromString("test"));
        primaryKeyMap.put("int", PrimaryKeyValue.fromLong(1));
        primaryKeyMap.put("bytes", PrimaryKeyValue.fromBinary("test".getBytes()));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals((byte) 1, columnsMap.get("byte").firstEntry().getValue().asLong());
        Assert.assertEquals((short) 1, columnsMap.get("short").firstEntry().getValue().asLong());
        Assert.assertEquals(1L, columnsMap.get("long").firstEntry().getValue().asLong());
        Assert.assertTrue(1.0f == columnsMap.get("float").firstEntry().getValue().asDouble());
        Assert.assertTrue(1.0 == columnsMap.get("double").firstEntry().getValue().asDouble());
        Assert.assertEquals(true, columnsMap.get("boolean").firstEntry().getValue().asBoolean());

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


