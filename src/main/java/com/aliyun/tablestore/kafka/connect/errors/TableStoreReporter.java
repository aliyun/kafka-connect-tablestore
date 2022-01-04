package com.aliyun.tablestore.kafka.connect.errors;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.*;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.RowWriteResult;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.enums.AuthMode;
import com.aliyun.tablestore.kafka.connect.model.StsUserBo;
import com.aliyun.tablestore.kafka.connect.service.StsService;
import com.aliyun.tablestore.kafka.connect.utils.ClientUtil;
import com.aliyun.tablestore.kafka.connect.utils.ParamChecker;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * TableStore错误报告器：需要指定表名
 * 表的主键为 {"topic_partition","offset"}，属性列为{key,value,error_info}
 */
public class TableStoreReporter extends GenericErrorReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreReporter.class);

    private final TableStoreSinkConfig config;

    private String endpoint;
    private String instanceName;

    private String tableName;
    private List<PrimaryKeySchema> primaryKeySchemaList;
    private boolean autoCreate;
    private DefaultTableStoreWriter writer;

    private String regionId;
    private String accountId;
    private String stsAccessId;
    private String stsAccessKey;
    private String roleName;
    private String stsEndpoint;

    private long clientTimeOutMs;
    private long clientCreateLastTime;

    private AuthMode authMode;

    private final String accessKeyId;
    private final String accessKeySecret;
    private CredentialsProvider credentialsProvider = null;
    private ExecutorService executorService = null;
    private AsyncClient ots = null;

    public TableStoreReporter(TableStoreSinkConfig config) {
        super();
        LOGGER.info("Initialize TableStore Error Reporter");
        this.config = config;

        authMode = AuthMode.getType(config.getString(TableStoreSinkConfig.TABLESTORE_AUTH_MODE));
        if (authMode == null) {
            LOGGER.error("auth mode is empty, please check");
            throw new RuntimeException("auth mode is empty, please check");
        }

        endpoint = config.getString(TableStoreSinkConfig.OTS_ENDPOINT);
        accessKeyId = config.getString(TableStoreSinkConfig.OTS_ACCESS_KEY_ID);
        accessKeySecret = config.getPassword(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET).value();

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

        regionId = config.getString(TableStoreSinkConfig.REGION);
        accountId = config.getString(TableStoreSinkConfig.ACCOUNT_ID);
        Map<String, String> env = System.getenv();
        stsAccessId = env.getOrDefault(TableStoreSinkConfig.STS_ACCESS_ID, "");
        stsAccessKey = env.getOrDefault(TableStoreSinkConfig.STS_ACCESS_KEY, "");
        roleName = config.getString(TableStoreSinkConfig.ROLE_NAME);
        stsEndpoint = config.getString(TableStoreSinkConfig.STS_ENDPOINT);
        clientTimeOutMs = config.getLong(TableStoreSinkConfig.CLIENT_TIME_OUT_MS);

        StsUserBo stsUserBo = createFakeStsUserBo(accessKeyId, accessKeySecret);
        if (AuthMode.STS == authMode) {
            stsUserBo = StsService.getAssumeRole(accountId, regionId, stsEndpoint, stsAccessId, stsAccessKey, roleName);
        }
        clientCreateLastTime = System.currentTimeMillis();

        validateOrCreateIfNecessary(tableName, stsUserBo);
        initWriter(stsUserBo);
    }



    private StsUserBo createFakeStsUserBo(String ak, String sk) {
        StsUserBo bo = new StsUserBo();
        bo.setAk(ak);
        bo.setSk(sk);
        return bo;
    }


    /**
     * 验证 error 表结构,如果表不存在,则创建表
     *
     * @param tableName
     */
    private void validateOrCreateIfNecessary(String tableName, StsUserBo stsUserBo) {
        SyncClient ots = new SyncClient(endpoint, stsUserBo.getAk(), stsUserBo.getSk(), instanceName, stsUserBo.getToken());

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

        ots.createTable(request);

        return tableMeta;
    }

    /**
     * 初始化 writer
     */
    private void initWriter(StsUserBo stsUserBo) {
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
        ServiceCredentials credentials = new DefaultCredentials(stsUserBo.getAk(), stsUserBo.getSk(), stsUserBo.getToken());

        ClientConfiguration cc = ClientUtil.getClientConfiguration(writerConfig);

        credentialsProvider = CredentialsProviderFactory.newDefaultCredentialProvider(credentials.getAccessKeyId(), credentials.getAccessKeySecret(), credentials.getSecurityToken());
        ots = new AsyncClient(endpoint, credentialsProvider, instanceName, cc, new ResourceManager(cc, null));

        executorService = ClientUtil.createThreadPool(writerConfig);
        writer = new DefaultTableStoreWriter(ots, tableName, writerConfig, callback, executorService);
    }

    @Override
    public String className() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void report(SinkRecord errantRecord, Throwable error) {
        String errorInfo = convertError(error);

        RowChange rowChange = convertToRowChange(errantRecord, errorInfo);

        if (System.currentTimeMillis() - clientCreateLastTime > clientTimeOutMs) {
            rebuildWriter();
        }

        this.writer.addRowChange(rowChange);
    }

    private void rebuildWriter() {
        if (authMode == AuthMode.STS) {
            writer.flush();
            ClientUtil.refreshCredential(accountId, regionId, stsEndpoint, stsAccessId, stsAccessKey, roleName, credentialsProvider);
            clientCreateLastTime = System.currentTimeMillis();
        } else {
            clientCreateLastTime = System.currentTimeMillis();
        }
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
        writer.close();
        executorService.shutdown();
        ots.shutdown();
    }

}
