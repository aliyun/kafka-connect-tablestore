package com.aliyun.tablestore.kafka.connect;

import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.auth.DefaultCredentials;
import com.alicloud.openservices.tablestore.core.auth.ServiceCredentials;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.WriterResult;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeNotRetryStrategy;
import com.alicloud.openservices.tablestore.writer.retry.CertainCodeRetryStrategy;
import com.aliyun.tablestore.kafka.connect.enums.RuntimeErrorTolerance;
import com.aliyun.tablestore.kafka.connect.model.ErrantSinkRecord;
import com.aliyun.tablestore.kafka.connect.parsers.EventParsingException;
import com.aliyun.tablestore.kafka.connect.utils.ParamChecker;
import com.aliyun.tablestore.kafka.connect.utils.RowChangeTransformer;
import com.aliyun.tablestore.kafka.connect.utils.TransformException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TableStoreSinkWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreSinkWriter.class);

    private final TableStoreSinkConfig config;

    private final RowChangeTransformer transformer;

    private Map<String, TableStoreWriter> writersByTable;
    private Map<String, Executor> executors;

    private final WriterConfig writerConfig;
    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final ServiceCredentials credentials;
    private final String instanceName;
    private final TableStoreCallback<RowChange, ConsumedCapacity> callback;

    private final boolean autoCreate;
    private final RuntimeErrorTolerance tolerance;
    private static AtomicLong transformFailedRecords;
    private static AtomicLong writeFailedRecords;

    private final AsyncClient ots;

    private long clientTimeOutMs = 10 * 60 * 1000L;
    private long clientCreateLastTime;

    public TableStoreSinkWriter(TableStoreSinkConfig config) {
        this.config = config;

        transformer = new RowChangeTransformer(config);

        writerConfig = createWriterConfig();
        writersByTable = new LinkedHashMap<>();//建立表名和TableStoreWriter的映射关系
        executors = new LinkedHashMap<>();//建立表名和线程池映射关系

        endpoint = config.getString(TableStoreSinkConfig.OTS_ENDPOINT);
        accessKeyId = config.getString(TableStoreSinkConfig.OTS_ACCESS_KEY_ID);
        accessKeySecret = config.getPassword(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET).value();
        credentials = new DefaultCredentials(accessKeyId, accessKeySecret);
        instanceName = config.getString(TableStoreSinkConfig.OTS_INSTANCE_NAME);

        ots = InitClient();

        callback = new TableStoreCallback<RowChange, ConsumedCapacity>() {
            @Override
            public void onCompleted(RowChange rowChange, ConsumedCapacity consumedCapacity) {
            }

            @Override
            public void onFailed(RowChange rowChange, Exception e) {
            }
        };

        autoCreate = config.getBoolean(TableStoreSinkConfig.AUTO_CREATE);
        tolerance = config.getParserErrorTolerance();

        transformFailedRecords = new AtomicLong();
        writeFailedRecords = new AtomicLong();
    }

    /**
     * 创建 TableStoreWriter 的配置
     */
    private WriterConfig createWriterConfig() {
        WriterConfig writerConfig = new WriterConfig();
        writerConfig.setCallbackThreadCount(config.getInt(TableStoreSinkConfig.MAX_THREAD_COUNT));//一个TableStoreWriter的最大回调线程数
        writerConfig.setConcurrency(config.getInt(TableStoreSinkConfig.MAX_CONCURRENCY));//一个TableStoreWriter的最大请求并发数
        writerConfig.setBufferSize(config.getInt(TableStoreSinkConfig.BUFFER_SIZE));//一个TableStoreWriter在内存中缓冲队列的大小
        writerConfig.setFlushInterval(config.getInt(TableStoreSinkConfig.FLUSH_INTERVAL));//一个TableStoreWrite对缓冲区的刷新时间间隔

        writerConfig.setBatchRequestType(BatchRequestType.BULK_IMPORT);// 底层构建BulkImportRequest做批量写
        writerConfig.setBucketCount(config.getInt(TableStoreSinkConfig.BUCKET_COUNT)); // 分筒数，提升串行写并发，未达机器瓶颈时与写入速率正相关

        boolean insertOrderEnable = config.getBoolean(TableStoreSinkConfig.INSERT_ORDER_ENABLE);
        if (insertOrderEnable) {
            writerConfig.setWriteMode(WriteMode.SEQUENTIAL);// 串行写（每个筒内串行写）
            writerConfig.setDispatchMode(DispatchMode.HASH_PRIMARY_KEY);// 基于主键哈希值做分筒，保证同主键落在一个桶内，有序写
        } else {
            writerConfig.setWriteMode(WriteMode.PARALLEL);// 并行写（每个筒内并行写）
            writerConfig.setDispatchMode(DispatchMode.ROUND_ROBIN);// 循环遍历分筒派发
        }
        return writerConfig;
    }

    /**
     * 初始化 Client
     */
    private AsyncClient InitClient() {
        ClientConfiguration cc = new ClientConfiguration();
        cc.setMaxConnections(writerConfig.getClientMaxConnections());
        switch (writerConfig.getWriterRetryStrategy()) {
            case CERTAIN_ERROR_CODE_NOT_RETRY:
                cc.setRetryStrategy(new CertainCodeNotRetryStrategy());
                break;
            case CERTAIN_ERROR_CODE_RETRY:
            default:
                cc.setRetryStrategy(new CertainCodeRetryStrategy());
        }
        return new AsyncClient(endpoint, credentials.getAccessKeyId(), credentials.getAccessKeySecret(), instanceName, cc, credentials.getSecurityToken());
    }

    public void initWriter(String topic) {
        Set<String> set = new HashSet<>();
        set.add(topic);
        initWriter(set);
    }
    /**
     * 初始化TableStoreWriter,建立表名与TableStoreWriter的映射
     *
     * @param topics
     */
    public void initWriter(Set<String> topics) {

        clientCreateLastTime = System.currentTimeMillis();

        for (String topic : topics) {

            String tableName = config.getTableNameByTopic(topic);
            if (writersByTable.containsKey(tableName)) {
                continue;
            }
            LOGGER.info(String.format("Initializing writer for table: %s.", tableName));

            validateOrCreateIfNecessary(tableName);

            Executor executor = createThreadPool();

            TableStoreWriter writer = new DefaultTableStoreWriter(ots, tableName, writerConfig, callback, executor);
            executors.put(tableName, executor);
            writersByTable.put(tableName, writer);
        }
    }

    /**
     * 刷新所有TableStoreWriter的缓冲
     */
    public void flushWriters() {
        for (Map.Entry<String, TableStoreWriter> entry : writersByTable.entrySet()) {
            entry.getValue().flush();
        }
    }

    /**
     * 关闭所有的TableStoreWriter和线程池
     */
    public void closeWriters() {
        LOGGER.info("Writers close");
        flushWriters();
        for (Map.Entry<String, TableStoreWriter> entry : writersByTable.entrySet()) {
            entry.getValue().close();
            ((ExecutorService) executors.get(entry.getKey())).shutdown();
        }
        writersByTable = new LinkedHashMap<>();//建立表名和TableStoreWriter的映射关系
        executors = new LinkedHashMap<>();//建立表名和线程池映射关系
    }

    /**
     * 写入 OTS 表
     *
     * @param records
     */
    public List<ErrantSinkRecord> write(Collection<SinkRecord> records) {
        List<ErrantSinkRecord> errantRecords = new LinkedList<>();//用来存放写入失败的sinkRecord

        List<SinkRecord> usedRecords = new ArrayList<>();
        List<Future<WriterResult>> futures = new ArrayList<>();

        for (SinkRecord record : records) {
            String topic = record.topic();
            String tableName = config.getTableNameByTopic(topic);
            TableStoreWriter writer = this.writersByTable.get(tableName);

            RowChange rowChange = null;
            try {
                rowChange = transformer.transform(tableName, record);
                Future<WriterResult> future = writer.addRowChangeWithFuture(rowChange);
                futures.add(future);
                usedRecords.add(record);
            } catch (TransformException | EventParsingException e) {
                LOGGER.debug(String.format("Failed to transform sink record.Total transform failed: %d", transformFailedRecords.incrementAndGet()));
                if (RuntimeErrorTolerance.NONE.equals(tolerance)) {
                    LOGGER.error("An error occurred while converting SinkRecord to RowChange.", e);
                    throw new ConnectException(e);
                } else {
                    errantRecords.add(new ErrantSinkRecord(record, e));
                }
            }

        }

        flushWriters();

        for (int i = 0; i < futures.size(); ++i) {
            try {
                WriterResult result = futures.get(i).get();
                if (!result.isAllSucceed()) {
                    LOGGER.debug(String.format("Failed to write sink record to TableStore.Total write failed: %d", writeFailedRecords.incrementAndGet()));
                    WriterResult.RowChangeStatus rowChangeStatus = result.getFailedRows().get(0);

                    if (RuntimeErrorTolerance.NONE.equals(tolerance)) {
                        LOGGER.error("An error occurred while converting SinkRecord to RowChange.", rowChangeStatus.getException());
                        throw new ConnectException(rowChangeStatus.getException());
                    } else {
                        errantRecords.add(new ErrantSinkRecord(usedRecords.get(i), rowChangeStatus.getException()));
                    }
                }
            } catch (InterruptedException |ExecutionException e) {
                LOGGER.error("An error occurred while writing.", e);
            }
        }
        return errantRecords;
    }


    /**
     * 基于用户机器核数，内部构建合适的线程池（N为机器核数）
     * core/max:    支持用户配置，默认：核数+1
     * blockQueue:  支持用户配置，1024
     * Reject:      CallerRunsPolicy
     */
    private ExecutorService createThreadPool() {
        int coreThreadCount = writerConfig.getCallbackThreadCount();
        int maxThreadCount = coreThreadCount;
        int queueSize = writerConfig.getCallbackThreadPoolQueueSize();

        ThreadFactory threadFactory = new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "writer-callback-" + counter.getAndIncrement());
            }
        };

        return new ThreadPoolExecutor(coreThreadCount, maxThreadCount, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue(queueSize), threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
    }


    /**
     * 验证表结构,如果表不存在,则创建表
     *
     * @param tableName
     */
    private void validateOrCreateIfNecessary(String tableName) {

        DescribeTableRequest request = new DescribeTableRequest();
        request.setTableName(tableName);
        DescribeTableResponse res = null;
        TableMeta tableMeta = null;

        int maxRetry = 20;
        while (maxRetry > 0) {
            Future<DescribeTableResponse> result = ots.describeTable(request, null);
            try {
                res = result.get();
                tableMeta = res.getTableMeta();
                break;
            } catch (TableStoreException e) {
                if ("OTSObjectNotExist".equals(e.getErrorCode())) {
                    if (autoCreate) {
                        try {
                            tableMeta = tryCreateTable(tableName);
                        } catch (Exception exception) {
                            LOGGER.error(String.format("Error while create table:%s, retry:%s", tableName,maxRetry), e);
                        }
                    } else {
                        throw new ConnectException(
                                String.format("Table %s is missing and auto-creation is disabled", tableName)
                        );
                    }
                } else {
                    LOGGER.error("An error occurred while validating the table", e);
                }
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("An error occurred while validating the table.", e);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }

            maxRetry--;
            if (maxRetry <= 0) {
                throw new ConnectException(String.format("Error while describe table:%s", tableName));
            }
        }

        List<PrimaryKeySchema> pkDefinedInConfig = config.getPrimaryKeySchemaListByTable(tableName);
        List<DefinedColumnSchema> colDefinedInConfig = config.getColumnSchemaListByTable(tableName);

        ParamChecker.checkTable(tableMeta, pkDefinedInConfig, colDefinedInConfig, writerConfig.getMaxColumnsCount());
    }

    /**
     * 创建表
     *
     * @param tableName
     */
    private TableMeta tryCreateTable(String tableName) {
        TableMeta tableMeta = new TableMeta(tableName);

        List<PrimaryKeySchema> primaryKeySchemaList = config.getPrimaryKeySchemaListByTable(tableName);

        tableMeta.addPrimaryKeyColumns(primaryKeySchemaList);

        TableOptions tableOptions = new TableOptions(-1, 1);
        CreateTableRequest request = new CreateTableRequest(
                tableMeta, tableOptions, new ReservedThroughput(new CapacityUnit(0, 0)));

        try {
            Future<CreateTableResponse> res = ots.createTable(request, null);
            CreateTableResponse response = res.get();
        } catch (InterruptedException | ExecutionException e) {
            LOGGER.error("An error occurred while creating the table.", e);
        }
        return tableMeta;
    }


    public Set<String> getAllTopics() {
        if (config == null) {
            return null;
        }
        return config.getAllTopics();
    }

    public static long getTransformFailedRecords() {
        return transformFailedRecords.get();
    }

    public static long getWriteFailedRecords() {
        return writeFailedRecords.get();
    }

    /**
     * 关闭 ots client
     */
    public void close() {
        LOGGER.debug("Client is closed.");
        ots.shutdown();
    }

    public boolean needRebuild() {
        return System.currentTimeMillis() - clientCreateLastTime > clientTimeOutMs;
    }

    public void rebuildClient() {
        // do nothing
        clientCreateLastTime = System.currentTimeMillis();
    }
}
