package functiontest;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.AccessKey;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkTask;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

public class InsertModeTest {
    private final String TOPIC = "test_insert_mode";
    private final int PARTITION = 0;

    private TopicPartition topicPartition;
    private Map<String, String> props;
    private TableStoreSinkTask task;

    private static final Schema KEY_SCHEMA;
    private static final Schema VALUE_SCHEMA;

    static {
        KEY_SCHEMA = SchemaBuilder.struct()
                .field("pk0", Schema.STRING_SCHEMA)
                .build();

        VALUE_SCHEMA = SchemaBuilder.struct()
                .field("A", Schema.STRING_SCHEMA)
                .field("B", Schema.INT32_SCHEMA)
                .field("C", Schema.OPTIONAL_BYTES_SCHEMA)
                .build();
    }

    private static SyncClient client;

    @Before
    public void before() {
        props = new HashMap<>();
        props.put("name", "kafka-tablestore-sink");
        props.put("connector.class", "TableStoreSinkConnector");
        props.put("task.max", "1");
        props.put(TableStoreSinkConfig.TOPIC_LIST, TOPIC);
        props.put(TableStoreSinkConfig.OTS_ENDPOINT, AccessKey.endpoint);
//        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_ID, AccessKey.accessKeyId);
//        props.put(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET, AccessKey.accessKeySecret);
        props.put(TableStoreSinkConfig.OTS_INSTANCE_NAME, AccessKey.instanceName);
        props.put(TableStoreSinkConfig.PRIMARY_KEY_MODE, "record_key");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_NAME_TEMPLATE, TOPIC), "pk0");
        props.put(String.format(TableStoreSinkConfig.PRIMARY_KEY_TYPE_TEMPLATE, TOPIC), "string");
        props.put(TableStoreSinkConfig.AUTO_CREATE, "true");

        topicPartition=new TopicPartition(TOPIC,PARTITION);
        task = new TableStoreSinkTask();


        client = new SyncClient(AccessKey.endpoint, AccessKey.accessKeyId, AccessKey.accessKeySecret, AccessKey.instanceName);
        try {
            client.deleteTable(new DeleteTableRequest(TOPIC));
        } catch (Exception ignore) {

        }

    }

    @After
    public void after() {
        client.shutdown();
    }

    @Test
    public void putTest() {
        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put("pk0", PrimaryKeyValue.fromString("pk_value"));


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "pk_value");

        Struct value = new Struct(VALUE_SCHEMA)
                .put("A", "test")
                .put("B", 1);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE,"put");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));
        putRow(primaryKeyMap);
        task.put(Collections.singletonList(sinkRecord));
        task.close(Collections.singletonList(topicPartition));
        task.stop();

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertTrue(columnsMap.get("C")==null);

    }

    @Test
    public void updateTest() {
        Map<String, PrimaryKeyValue> primaryKeyMap = new LinkedHashMap<>();
        primaryKeyMap.put("pk0", PrimaryKeyValue.fromString("pk_value"));


        Struct key = new Struct(KEY_SCHEMA)
                .put("pk0", "pk_value");

        Struct value = new Struct(VALUE_SCHEMA)
                .put("A", "test")
                .put("B", 1);

        SinkRecord sinkRecord = new SinkRecord(TOPIC, PARTITION, KEY_SCHEMA, key, VALUE_SCHEMA, value, 0L);

        props.put(TableStoreSinkConfig.INSERT_MODE,"update");
        task.start(props);
        task.open(Collections.singletonList(topicPartition));
        putRow(primaryKeyMap);
        task.put(Collections.singletonList(sinkRecord));
        task.close(Collections.singletonList(topicPartition));
        task.stop();

        Row row = getRow(primaryKeyMap);
        NavigableMap<String, NavigableMap<Long, ColumnValue>> columnsMap = row.getColumnsMap();

        Assert.assertTrue(columnsMap.get("C")!=null);
    }

    public void putRow(Map<String, PrimaryKeyValue> primaryKeyMap) {
        //构造主键。
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        for (Map.Entry<String, PrimaryKeyValue> entry : primaryKeyMap.entrySet()) {
            primaryKeyBuilder.addPrimaryKeyColumn(entry.getKey(), entry.getValue());
        }
        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowPutChange = new RowPutChange(TOPIC, primaryKey);

        rowPutChange.addColumn("A", ColumnValue.fromString("test"));
        rowPutChange.addColumn("B", ColumnValue.fromLong(1));
        rowPutChange.addColumn("C", ColumnValue.fromBinary("test".getBytes()));

        client.putRow(new PutRowRequest(rowPutChange));

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
