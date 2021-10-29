package functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import com.aliyun.tablestore.kafka.connect.AccessKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

public class DeleteModeTest {
    private final String TOPIC = "test_delete";
    private final int PARTITION = 0;
    private TableStoreSinkWriter tableStoreSinkWriter;
    private Map<String, String> props;
    private SyncClient client;

    private static final Schema KEY_SCHEMA;
    private static final Schema VALUE_SCHEMA;
    private static final Schema VALUE_SCHEMA2;

    static {
        KEY_SCHEMA = SchemaBuilder.struct()
                .field("pk0", Schema.STRING_SCHEMA)
                .build();
        VALUE_SCHEMA= SchemaBuilder.struct()
                .field("A", Schema.OPTIONAL_STRING_SCHEMA)
                .field("B", Schema.OPTIONAL_INT32_SCHEMA)
                .field("C", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();

        VALUE_SCHEMA2 = SchemaBuilder.struct()
                .field("A",Schema.STRING_SCHEMA)
                .build();
    }

    @Before
    public void before() {
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

        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "record_key");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "pk0");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "string");
        props.put(TableStoreSinkConfig.RUNTIME_ERROR_TOLERANCE,"all");

        client = new SyncClient(AccessKey.endpoint, AccessKey.accessKeyId, AccessKey.accessKeySecret, AccessKey.instanceName);
    }

    @After
    public void after() {
        if (tableStoreSinkWriter != null) {
            tableStoreSinkWriter.close();
        }
        client.shutdown();
    }

    /**
     * insertMode=put,deleteMode=none覆盖写
     */
    @Test
    public void writeNullValueWithPutAndNone() {
        String insertMode="put";
        String deleteMode="none";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row.getColumnsMap().isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=column 覆盖写
     */
    @Test
    public void writeNullValueWithPutAndColumn() {
        String insertMode="put";
        String deleteMode="column";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row.getColumnsMap().isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=row 删行
     */
    @Test
    public void writeNullValueWithPutAndRow() {
        String insertMode="put";
        String deleteMode="row";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");


        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row==null);
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=row_and_column 删行
     */
    @Test
    public void writeNullValueWithPutAndBoth() {
        String insertMode="put";
        String deleteMode="row_and_column";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row==null);
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=none 脏数据
     */
    @Test
    public void writeNullValueWithUpdateAndNone() {
        String insertMode="update";
        String deleteMode="none";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row!=null);
        Assert.assertEquals(1, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=column 脏数据
     */
    @Test
    public void writeNullValueWithUpdateAndColumn() {
        String insertMode="update";
        String deleteMode="none";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row!=null);
        Assert.assertEquals(1, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=row 删行
     */
    @Test
    public void writeNullValueWithUpdateAndRow() {
        String insertMode="update";
        String deleteMode="row";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row==null);
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=row_and_column 删行
     */
    @Test
    public void writeNullValueWithUpdateAndBoth() {
        String insertMode="update";
        String deleteMode="row_and_column";

        putRow("value_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("value_null");
        Assert.assertTrue(row==null);
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=none 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndNone() {
        String insertMode="put";
        String deleteMode="none";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Assert.assertTrue(row.getColumnsMap().isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=row 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndRow() {
        String insertMode="put";
        String deleteMode="row";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Assert.assertTrue(row.getColumnsMap().isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=column 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndColumn() {
        String insertMode="put";
        String deleteMode="column";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Assert.assertTrue(row.getColumnsMap().isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=row_and_column 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndBoth() {
        String insertMode="put";
        String deleteMode="row_and_column";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Assert.assertTrue(row.getColumnsMap().isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=none 脏数据
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndNone() {
        String insertMode="update";
        String deleteMode="none";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Assert.assertFalse(row==null);
        Assert.assertEquals(1, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=row 脏数据
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndRow() {
        String insertMode="update";
        String deleteMode="row";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Assert.assertFalse(row==null);
        Assert.assertEquals(1, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=column 删列
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndColumn() {
        String insertMode="update";
        String deleteMode="column";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");

        Assert.assertFalse(row.contains("A"));
        Assert.assertFalse(row.contains("B"));
        Assert.assertFalse(row.contains("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=row_and_column 删列
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndBoth() {
        String insertMode="update";
        String deleteMode="row_and_column";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");

        Assert.assertFalse(row.contains("A"));
        Assert.assertFalse(row.contains("B"));
        Assert.assertFalse(row.contains("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=none 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndNone() {
        String insertMode="put";
        String deleteMode="none";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertFalse(res.containsKey("B"));
        Assert.assertFalse(res.containsKey("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=row 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndRow() {
        String insertMode="put";
        String deleteMode="row";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertFalse(res.containsKey("B"));
        Assert.assertFalse(res.containsKey("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=column 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndColumn() {
        String insertMode="put";
        String deleteMode="column";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertFalse(res.containsKey("B"));
        Assert.assertFalse(res.containsKey("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put,deleteMode=row_and_column 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndBoth() {
        String insertMode="put";
        String deleteMode="row_and_column ";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertFalse(res.containsKey("B"));
        Assert.assertFalse(res.containsKey("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=none 忽略空值
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndNone() {
        String insertMode="update";
        String deleteMode="none";

        putRow("all_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("all_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertEquals(1,res.get("B").firstEntry().getValue().asLong());
        Assert.assertEquals("test",new String(res.get("C").firstEntry().getValue().asBinary()));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=row 忽略空值
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndRow() {
        String insertMode="update";
        String deleteMode="row";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertEquals(1,res.get("B").firstEntry().getValue().asLong());
        Assert.assertEquals("test",new String(res.get("C").firstEntry().getValue().asBinary()));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=column 删列
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndColumn() {
        String insertMode="update";
        String deleteMode="column";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertFalse(res.containsKey("B"));
        Assert.assertFalse(res.containsKey("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,deleteMode=row_and_column 删列
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndBoth() {
        String insertMode="update";
        String deleteMode="row_and_column";

        putRow("partial_fields_null");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("partial_fields_null");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertFalse(res.containsKey("B"));
        Assert.assertFalse(res.containsKey("C"));
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put, 有白名单，写入
     */
    @Test
    public void writeNullFieldsInWhiteListWithPutAndDefault() {
        String insertMode="put";

        putRow("whitelist");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "whitelist");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);

        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_NAME_TEMPLATE, TOPIC), "A,B");
        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_TYPE_TEMPLATE, TOPIC), "string,integer");

        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("whitelist");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertTrue(res.isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update, 有白名单，对其它列不做更新
     */
    @Test
    public void writeNullFieldsInWhiteListWithUpdateAndDefault() {
        String insertMode="update";

        putRow("whitelist");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "whitelist");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);

        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_NAME_TEMPLATE, TOPIC), "A,B");
        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_TYPE_TEMPLATE, TOPIC), "string,integer");

        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("whitelist");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertEquals(1,res.get("B").firstEntry().getValue().asLong());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=put, 覆盖写
     */
    @Test
    public void writeMissingFieldsInWhiteListWithPut() {
        String insertMode="put";

        putRow("whitelist");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "whitelist");

        Struct value=new Struct(VALUE_SCHEMA2);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA2, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);

        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_NAME_TEMPLATE, TOPIC), "A,B");
        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_TYPE_TEMPLATE, TOPIC), "string,integer");

        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("whitelist");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertTrue(res.isEmpty());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    /**
     * insertMode=update,更新存在的字段
     */
    @Test
    public void writeMissingFieldsInWhiteListWithUpdateAndDefault() {
        String insertMode="update";

        putRow("whitelist");

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "whitelist");

        Struct value=new Struct(VALUE_SCHEMA2)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA2, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);

        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_NAME_TEMPLATE, TOPIC), "A,B");
        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_TYPE_TEMPLATE, TOPIC), "string,integer");

        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        tableStoreSinkWriter = new TableStoreSinkWriter(config);
        tableStoreSinkWriter.initWriter(TOPIC);
        tableStoreSinkWriter.write(Collections.singletonList(sinkRecord));
        tableStoreSinkWriter.closeWriters();
        tableStoreSinkWriter.close();
        Row row = getRow("whitelist");
        Map<String, NavigableMap<Long, ColumnValue>> res=row.getColumnsMap();

        Assert.assertEquals("update",res.get("A").firstEntry().getValue().asString());
        Assert.assertEquals(1,res.get("B").firstEntry().getValue().asLong());
        Assert.assertEquals(0, tableStoreSinkWriter.getTransformFailedRecords());
    }

    public void putRow(String pkValue) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromString(pkValue));
        PrimaryKey primaryKey = primaryKeyBuilder.build();
        //设置数据表名称。
        RowPutChange rowPutChange = new RowPutChange(TOPIC, primaryKey);

        rowPutChange.addColumn(new Column("A",ColumnValue.fromString("test")));
        rowPutChange.addColumn(new Column("B",ColumnValue.fromLong(1)));
        rowPutChange.addColumn(new Column("C",ColumnValue.fromBinary("test".getBytes())));
        client.putRow(new PutRowRequest(rowPutChange));
    }

    public Row getRow(String pkValue) {
        //构造主键。
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        primaryKeyBuilder.addPrimaryKeyColumn("pk0", PrimaryKeyValue.fromString(pkValue));
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
