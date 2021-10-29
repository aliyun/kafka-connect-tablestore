package com.aliyun.tablestore.kafka.connect.errors;

import com.alicloud.openservices.tablestore.DefaultTableStoreWriter;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreCallback;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.utils.ParamChecker;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * TableStore错误报告器：需要指定表名
 * 表的主键为 {"topic_partition","offset"}，属性列为{key,value,error_info}
 */
public class TableStoreReporter extends GenericErrorReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreReporter.class);

    private final TableStoreSinkConfig config;

    private String endpoint;
    private String accessKeyId;
    private String accessKeySecret;
    private ServiceCredentials credentials;
    private String instanceName;

    private String tableName;
    private List<PrimaryKeySchema> primaryKeySchemaList;
    private boolean autoCreate;
    private DefaultTableStoreWriter writer;


    public TableStoreReporter(TableStoreSinkConfig config) {
        super();
        LOGGER.info("Initialize TableStore Error Reporter");
        this.config = config;

        endpoint = config.getString(TableStoreSinkConfig.OTS_ENDPOINT);
        accessKeyId = config.getString(TableStoreSinkConfig.OTS_ACCESS_KEY_ID);
        accessKeySecret = config.getPassword(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET).value();
        credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
        instanceName = config.getString(TableStoreSinkConfig.OTS_INSTANCE_NAME);

        tableName = config.getString(TableStoreSinkConfig.RUNTIME_ERROR_TABLE_NAME);

        primaryKeySchemaList = new ArrayList<>();
        primaryKeySchemaList.add(
                new PrimaryKeySchema(TableStoreSinkConfig.PRIMARY_KEY_NAME_TOPIC_PARTITION, PrimaryKeyType.STRING)
        );
        primaryKeySchemaList.add(
                new PrimaryKeySchema(TableStoreSinkConfig.PRIMARY_KEY_NAME_OFFSET, PrimaryKeyType.INTEGER)
        );

        autoCreate = config.getBoolean(TableStoreSinkConfig.AUTO_CREATE);

        validateOrCreateIfNecessary(tableName);

        initWriter();
    }


    /**
     * 验证 error 表结构,如果表不存在,则创建表
     *
     * @param tableName
     */
    private void validateOrCreateIfNecessary(String tableName) {
        SyncClient ots = new SyncClient(endpoint, accessKeyId, accessKeySecret, instanceName);

        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(tableName);

        DescribeTableResponse res = null;
        TableMeta tableMeta = null;

        try {
            res = ots.describeTable(request);
            tableMeta = res.getTableMeta();
        } catch (TableStoreException e) {
            if ("OTSObjectNotExist".equals(e.getErrorCode())) {
                if (autoCreate) {
                    tableMeta = tryCreateTable(ots, tableName);
                }else{
                    throw new ConnectException(
                            String.format("Table %s is missing and auto-creation is disabled", tableName)
                    );
                }
            } else {
                LOGGER.error("An error occurred while creating the table.", e);
            }
        }
        ParamChecker.checkPrimaryKey(tableMeta.getPrimaryKeyList(), primaryKeySchemaList);
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    private TableMeta tryCreateTable(SyncClient ots, String tableName) {
        TableMeta tableMeta = new TableMeta(tableName);

        tableMeta.addPrimaryKeyColumns(primaryKeySchemaList);

        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(
                tableMeta, tableOptions, new ReservedThroughput(new CapacityUnit(0, 0)));

        CreateTableResponse res = ots.createTable(request);

        return tableMeta;
    }

    /**
     * 初始化 writer
     */
    private void initWriter() {
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setConcurrency(config.getInt(TableStoreSinkConfig.MAX_CONCURRENCY));//一个TableStoreWriter的最大请求并发数
        writerConfig.setBufferSize(config.getInt(TableStoreSinkConfig.BUFFER_SIZE));//一个TableStoreWriter在内存中缓冲队列的大小
        writerConfig.setFlushInterval(config.getInt(TableStoreSinkConfig.FLUSH_INTERVAL));//一个TableStoreWrite对缓冲区的刷新时间间隔
        writerConfig.setBatchRequestType(BatchRequestType.BULK_IMPORT);// 底层构建BulkImportRequest做批量写
        writerConfig.setBucketCount(config.getInt(TableStoreSinkConfig.BUCKET_COUNT)); // 分筒数，提升串行写并发，未达机器瓶颈时与写入速率正相关
        writerConfig.setCallbackThreadCount(1);//写入进程的回调处理线程数
        writerConfig.setWriteMode(WriteMode.PARALLEL);// 并行写（每个筒内并行写）
        writerConfig.setDispatchMode(DispatchMode.ROUND_ROBIN);// 循环遍历分筒派发

        TableStoreCallback callback = new TableStoreCallback<RowChange, RowWriteResult>() {

            public void onCompleted(RowChange rowChange, RowWriteResult cc) {
                LOGGER.info("Succeed to write message to runtime error table.Table Name=" + rowChange.getTableName());
            }

            public void onFailed(RowChange rowChange, Exception ex) {
                LOGGER.error("Could not add message to runtime error table. Table Name=" + tableName, ex);
            }
        };


        writer = new DefaultTableStoreWriter(
                endpoint, credentials, instanceName, tableName, writerConfig, callback);

    }

    @Override
    public String className() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void report(SinkRecord errantRecord, Throwable error) {
        String errorInfo = convertError(error);

        RowChange rowChange = convertToRowChange(errantRecord, errorInfo);

        this.writer.addRowChange(rowChange);
    }

    /**
     * 转换为 rowChange
     *
     * @param errantRecord
     * @param errorInfo
     */
    private RowChange convertToRowChange(SinkRecord errantRecord, String errorInfo) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn(
                        primaryKeySchemaList.get(0).getName(),
                        PrimaryKeyValue.fromString(String.format("%s_%s", errantRecord.topic(), errantRecord.kafkaPartition()))
                )
                .addPrimaryKeyColumn(
                        primaryKeySchemaList.get(1).getName(),
                        PrimaryKeyValue.fromLong(errantRecord.kafkaOffset())
                );

        PrimaryKey primaryKey = primaryKeyBuilder.build();

        RowPutChange rowChange = new RowPutChange(tableName, primaryKey);

        rowChange.addColumn("error_info", ColumnValue.fromString(errorInfo));

        byte[] key = this.keyConverter.fromConnectData(tableName, errantRecord.keySchema(), errantRecord.key());
        byte[] value = this.valueConverter.fromConnectData(tableName, errantRecord.valueSchema(), errantRecord.value());

        if (key != null) {
            rowChange.addColumn("key", ColumnValue.fromBinary(key));
        }
        if (value != null) {
            rowChange.addColumn("value", ColumnValue.fromBinary(value));
        }
        return rowChange;
    }


    @Override
    public void flush() {
        this.writer.flush();
    }

    @Override
    public void close() {
        LOGGER.info("Shutdown TableStore Error Reporter");
        this.writer.close();
    }
}
