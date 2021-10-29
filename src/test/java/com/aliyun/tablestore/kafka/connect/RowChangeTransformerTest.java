package com.aliyun.tablestore.kafka.connect;

import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowUpdateChange;
import com.aliyun.tablestore.kafka.connect.utils.RowChangeTransformer;
import com.aliyun.tablestore.kafka.connect.utils.TransformException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.fail;

public class RowChangeTransformerTest {
    private final String TOPIC = "test";
    private final int PARTITION = 0;

    private Map<String, String> props;
    private RowChangeTransformer transformer;

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
        props.put("name", "kafka-tablestore-sink");
        props.put("connector.class", "TableStoreSinkConnector");
        props.put("task.max", "1");
        props.put(TableStoreSinkConfig.TOPIC_LIST, TOPIC);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);

        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "record_key");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "pk0");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "string");
    }

    @Test
    public void testRowPutChange() {

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "pk_value");

        Struct value = new Struct(VALUE_SCHEMA)
                .put("A", "test")
                .put("B", 1);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE,"put");
        TableStoreSinkConfig config=new TableStoreSinkConfig(props);

        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    @Test
    public void updateTest() {
        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "pk_value");

        Struct value = new Struct(VALUE_SCHEMA)
                .put("A", "test")
                .put("B", 1);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE,"update");
        TableStoreSinkConfig config=new TableStoreSinkConfig(props);

        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());

    }

    /**
     * insertMode=put,deleteMode=none覆盖写
     */
    @Test
    public void writeNullValueWithPutAndNone() {
        String insertMode="put";
        String deleteMode="none";


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);

        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=column 覆盖写
     */
    @Test
    public void writeNullValueWithPutAndColumn() {
        String insertMode="put";
        String deleteMode="column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);

        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=row 删行
     */
    @Test
    public void writeNullValueWithPutAndRow() {
        String insertMode="put";
        String deleteMode="row";


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");


        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowDeleteChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=row_and_column 删行
     */
    @Test
    public void writeNullValueWithPutAndBoth() {
        String insertMode="put";
        String deleteMode="row_and_column";


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowDeleteChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=update,deleteMode=none 脏数据
     */
    @Test
    public void writeNullValueWithUpdateAndNone() {
        String insertMode="update";
        String deleteMode="none";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        try{
            RowChange rowChange=transformer.transform(TOPIC,sinkRecord);
            fail();
        }catch (TransformException e) {
        }
    }

    /**
     * insertMode=update,deleteMode=column 脏数据
     */
    @Test
    public void writeNullValueWithUpdateAndColumn() {
        String insertMode="update";
        String deleteMode="none";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        try{
            RowChange rowChange=transformer.transform(TOPIC,sinkRecord);
            fail();
        }catch (TransformException e) {
        }
    }

    /**
     * insertMode=update,deleteMode=row 删行
     */
    @Test
    public void writeNullValueWithUpdateAndRow() {
        String insertMode="update";
        String deleteMode="row";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowDeleteChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=update,deleteMode=row_and_column 删行
     */
    @Test
    public void writeNullValueWithUpdateAndBoth() {
        String insertMode="update";
        String deleteMode="row_and_column";


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "value_null");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, null, null, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowDeleteChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=none 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndNone() {
        String insertMode="put";
        String deleteMode="none";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=row 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndRow() {
        String insertMode="put";
        String deleteMode="row";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=column 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndColumn() {
        String insertMode="put";
        String deleteMode="column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=row_and_column 覆盖写
     */
    @Test
    public void writeAllFieldsNullWithPutAndBoth() {
        String insertMode="put";
        String deleteMode="row_and_column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=update,deleteMode=none 脏数据
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndNone() {
        String insertMode="update";
        String deleteMode="none";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        try{
            RowChange rowChange=transformer.transform(TOPIC,sinkRecord);
            fail();
        }catch (TransformException e){

        }

    }

    /**
     * insertMode=update,deleteMode=row 脏数据
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndRow() {
        String insertMode="update";
        String deleteMode="row";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        try{
            RowChange rowChange=transformer.transform(TOPIC,sinkRecord);
            fail();
        }catch (TransformException e){

        }
    }

    /**
     * insertMode=update,deleteMode=column 删列
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndColumn() {
        String insertMode="update";
        String deleteMode="column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;

        Assert.assertEquals(RowUpdateChange.Type.DELETE_ALL,rowUpdateChange.getColumnsToUpdate().get(0).getSecond());
    }

    /**
     * insertMode=update,deleteMode=row_and_column 删列
     */
    @Test
    public void writeAllFieldsNullWithUpdateAndBoth() {
        String insertMode="update";
        String deleteMode="row_and_column";
        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;

        Assert.assertEquals(RowUpdateChange.Type.DELETE_ALL,rowUpdateChange.getColumnsToUpdate().get(0).getSecond());

    }

    /**
     * insertMode=put,deleteMode=none 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndNone() {
        String insertMode="put";
        String deleteMode="none";


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=row 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndRow() {
        String insertMode="put";
        String deleteMode="row";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());

    }

    /**
     * insertMode=put,deleteMode=column 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndColumn() {
        String insertMode="put";
        String deleteMode="column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);
        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=put,deleteMode=row_and_column 覆盖写
     */
    @Test
    public void writePartialFieldsNullWithPutAndBoth() {
        String insertMode="put";
        String deleteMode="row_and_column ";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());
    }

    /**
     * insertMode=update,deleteMode=none 忽略空值
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndNone() {
        String insertMode="update";
        String deleteMode="none";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "all_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;

        Assert.assertEquals(1,rowUpdateChange.getColumnsToUpdate().size());
    }

    /**
     * insertMode=update,deleteMode=row 忽略空值
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndRow() {
        String insertMode="update";
        String deleteMode="row";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;

        Assert.assertEquals(1,rowUpdateChange.getColumnsToUpdate().size());
    }

    /**
     * insertMode=update,deleteMode=column 删列
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndColumn() {
        String insertMode="update";
        String deleteMode="column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;

        Assert.assertEquals(3,rowUpdateChange.getColumnsToUpdate().size());
        Assert.assertEquals(RowUpdateChange.Type.DELETE_ALL,rowUpdateChange.getColumnsToUpdate().get(1).getSecond());
    }

    /**
     * insertMode=update,deleteMode=row_and_column 删列
     */
    @Test
    public void writePartialFieldsNullWithUpdateAndBoth() {
        String insertMode="update";
        String deleteMode="row_and_column";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "partial_fields_null");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);
        props.put(TableStoreSinkConfig.DELETE_MODE, deleteMode);
        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;
        Assert.assertEquals(3,rowUpdateChange.getColumnsToUpdate().size());
        Assert.assertEquals(RowUpdateChange.Type.DELETE_ALL,rowUpdateChange.getColumnsToUpdate().get(1).getSecond());
    }

    /**
     * insertMode=put, 有白名单，写入
     */
    @Test
    public void writeNullFieldsInWhiteListWithPut() {
        String insertMode="put";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "whitelist");

        Struct value=new Struct(VALUE_SCHEMA);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);

        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_NAME_TEMPLATE, TOPIC), "A,B");
        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_TYPE_TEMPLATE, TOPIC), "string,integer");

        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowPutChange",rowChange.getClass().getSimpleName());

    }

    /**
     * insertMode=update, 有白名单，对其它列不做更新
     */
    @Test
    public void writeNullFieldsInWhiteListWithUpdate() {
        String insertMode="update";

        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "whitelist");

        Struct value=new Struct(VALUE_SCHEMA)
                .put("A","update");

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE, insertMode);

        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_NAME_TEMPLATE, TOPIC), "A,B");
        props.put(String.format(TableStoreSinkConfig.COLUMNS_WHITELIST_TYPE_TEMPLATE, TOPIC), "string,integer");

        TableStoreSinkConfig config = new TableStoreSinkConfig(props);
        transformer=new RowChangeTransformer(config);
        RowChange rowChange=transformer.transform(TOPIC,sinkRecord);

        Assert.assertEquals("RowUpdateChange",rowChange.getClass().getSimpleName());
        RowUpdateChange rowUpdateChange=(RowUpdateChange)rowChange;
        Assert.assertEquals(1,rowUpdateChange.getColumnsToUpdate().size());
    }
}
