package functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.core.utils.Bytes;
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

public class MapSchemaTest {
    String TOPIC = "test_map";
    int PARTITION = 0;
    TopicPartition topicPartition;
    Map<String, String> props;
    SyncClient client;
    TableStoreSinkTask task;
    JsonConverter jsonConverter;

    private static final Schema VALUE_SCHEMA;
    private static final Schema MAP_STRING_SCHEMA;
    private static final Schema MAP_BYTE_SCHEMA;
    private static final Schema MAP_SHORT_SCHEMA;
    private static final Schema MAP_INTEGER_SCHEMA;
    private static final Schema MAP_LONG_SCHEMA;
    private static final Schema MAP_FLOAT_SCHEMA;
    private static final Schema MAP_DOUBLE_SCHEMA;
    private static final Schema MAP_BOOLEAN_SCHEMA;
    private static final Schema MAP_BYTES_SCHEMA;

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

        MAP_STRING_SCHEMA = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.STRING_SCHEMA
        ).build();

        MAP_BYTE_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA,Schema.INT8_SCHEMA
        ).build();

        MAP_SHORT_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.INT16_SCHEMA
        ).build();

        MAP_INTEGER_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA,Schema.INT32_SCHEMA
        ).build();

        MAP_LONG_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA,Schema.INT64_SCHEMA
        ).build();

        MAP_FLOAT_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA,Schema.FLOAT32_SCHEMA
        ).build();

        MAP_DOUBLE_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA,Schema.FLOAT64_SCHEMA
        ).build();

        MAP_BOOLEAN_SCHEMA=SchemaBuilder.map(
                Schema.STRING_SCHEMA,Schema.BOOLEAN_SCHEMA
        ).build();

        MAP_BYTES_SCHEMA = SchemaBuilder.map(
                Schema.STRING_SCHEMA, Schema.BYTES_SCHEMA
        ).build();
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
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");

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
    public void testNullSchemaStringPk() {
        try{
            client.deleteTable(new DeleteTableRequest(TOPIC));
        }catch(Exception ignore){

        }
        String pkName = "string";
        String pkValue = "test";

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
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
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(pkValue.getBytes()));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaBytePk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }

        String pkName = "byte";
        byte pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
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
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes((long) pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaShortPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "short";
        short pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
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
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes((long) pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaIntPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "int";
        int pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
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
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes((long) pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaLongPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "long";
        long pkValue = 1L;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
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
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaFloatPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "float";
        float pkValue = 1.0f;

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
                .put("float", pkValue)
                .put("double", 1.0)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes((double) pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaDoublePk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "double";
        double pkValue = 1.0;

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
                .put("double", pkValue)
                .put("boolean", true)
                .put("bytes", "test".getBytes());

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();
        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaBooleanPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "boolean";
        boolean pkValue = true;

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
                .put("boolean", pkValue)
                .put("bytes", "test".getBytes());

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, VALUE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testNullSchemaMultiPk() {
        try{
            client.deleteTable(new DeleteTableRequest(TOPIC));
        }catch(Exception ignore){

        }

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "string,int,double,boolean");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary,binary,binary,binary");
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
        primaryKeyMap.put("string", PrimaryKeyValue.fromBinary("test".getBytes()));
        primaryKeyMap.put("int", PrimaryKeyValue.fromBinary(Bytes.toBytes((long)1)));
        primaryKeyMap.put("double", PrimaryKeyValue.fromBinary(Bytes.toBytes(1.0)));
        primaryKeyMap.put("boolean", PrimaryKeyValue.fromBinary(Bytes.toBytes(true)));

        Row row = getRow(primaryKeyMap);

        Assert.assertTrue(row!=null);
    }


    @Test
    public void testMapSchemaStringPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "string";
        String pkValue = "test";

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, String> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1", "test1");
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_STRING_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test1", Bytes.toString(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaBytePk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "byte";
        byte pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Byte> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1", (byte)2);
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_BYTE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(new byte[]{pkValue}));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals((byte)2, columnsMap.get("col1").firstEntry().getValue().asBinary()[0]);
    }

    @Test
    public void testMapSchemaShortPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "short";
        short pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Short> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1", (short)2);

        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_SHORT_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals((short)2, Bytes.toShort(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }


    @Test
    public void testMapSchemaIntegerPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "integer";
        int pkValue = 1;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Integer> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1",2);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, MAP_INTEGER_SCHEMA, value, 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals(2, Bytes.toInt(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaLongPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "long";
        long pkValue = 1L;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Long> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1",2L);
        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_LONG_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals(2L, Bytes.toLong(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaFloatPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "float";
        float pkValue = 1.0f;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Float> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1",2.0f);

        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_FLOAT_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertTrue(2.0f==Bytes.toFloat(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaDoublePk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "double";
        double pkValue = 1.0;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Double> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1",2.0);

        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_DOUBLE_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertTrue(2.0==Bytes.toDouble(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaBooleanPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }
        String pkName = "boolean";
        boolean pkValue = true;

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, Boolean> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1",false);

        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_BOOLEAN_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(Bytes.toBytes(pkValue)));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals(false,Bytes.toBoolean(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaBytesPk() {
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

        Map<String, byte[]> value = new HashMap<>();
        value.put(pkName, pkValue);
        value.put("col1", "test1".getBytes());

        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_BYTES_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put(pkName, PrimaryKeyValue.fromBinary(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test1", new String(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }

    @Test
    public void testMapSchemaMultiPk() {
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "pk0,pk1");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "binary,binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Map<String, String> value = new HashMap<>();
        value.put("pk0", "test0");
        value.put("pk1", "test1");
        value.put("col1", "test2");

        Map<String, String> jsonProps = new HashMap<>();
        jsonProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        jsonProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        jsonConverter.configure(jsonProps);

        byte[] bytes = jsonConverter.fromConnectData(TOPIC, MAP_STRING_SCHEMA, value);

        SchemaAndValue schemaAndValue = jsonConverter.toConnectData(TOPIC, bytes);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0L);

        task.put(Collections.singletonList(sinkRecord));

        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put("pk0", PrimaryKeyValue.fromBinary("test0".getBytes()));
        primaryKeyMap.put("pk1", PrimaryKeyValue.fromBinary("test1".getBytes()));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test2", new String(columnsMap.get("col1").firstEntry().getValue().asBinary()));
    }


   /* @Test
    public void testNullSchemaBytesPk(){
        String pkName="bytes";
        byte[] pkValue="test".getBytes();

        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE,TOPIC),pkName);
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE,TOPIC),"binary");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));

        Struct value = new Struct(VALUE_SCHEMA)
                .put("string", "test")
                .put("byte", (byte)1)
                .put("short",(short)1)
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

        Map<String,PrimaryKeyValue> primaryKeyMap=new LinkedHashMap<>();
        primaryKeyMap.put(pkName,PrimaryKeyValue.fromBinary(pkValue));

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertEquals("test", Bytes.toString(columnsMap.get("string").firstEntry().getValue().asBinary()));
        Assert.assertEquals((byte) 1, Bytes.toLong(columnsMap.get("byte").firstEntry().getValue().asBinary()));
        Assert.assertEquals((short) 1, Bytes.toLong(columnsMap.get("short").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1, Bytes.toLong(columnsMap.get("int").firstEntry().getValue().asBinary()));
        Assert.assertEquals(1L, Bytes.toLong(columnsMap.get("long").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0f == Bytes.toDouble(columnsMap.get("float").firstEntry().getValue().asBinary()));
        Assert.assertTrue(1.0 == Bytes.toDouble(columnsMap.get("double").firstEntry().getValue().asBinary()));
        Assert.assertEquals(true, Bytes.toBoolean(columnsMap.get("boolean").firstEntry().getValue().asBinary()));
    }*/

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
