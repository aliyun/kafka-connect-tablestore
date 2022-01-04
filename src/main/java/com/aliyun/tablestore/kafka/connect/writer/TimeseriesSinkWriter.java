package com.aliyun.tablestore.kafka.connect.writer;


import com.alicloud.openservices.tablestore.*;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProviderFactory;
import com.alicloud.openservices.tablestore.model.*;
import com.alicloud.openservices.tablestore.model.timeseries.*;
import com.alicloud.openservices.tablestore.writer.WriterConfig;
import com.alicloud.openservices.tablestore.writer.enums.BatchRequestType;
import com.alicloud.openservices.tablestore.writer.enums.DispatchMode;
import com.alicloud.openservices.tablestore.writer.enums.WriteMode;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.enums.AuthMode;
import com.aliyun.tablestore.kafka.connect.enums.RuntimeErrorTolerance;
import com.aliyun.tablestore.kafka.connect.model.ErrantSinkRecord;
import com.aliyun.tablestore.kafka.connect.model.StsUserBo;
import com.aliyun.tablestore.kafka.connect.parsers.EventParsingException;
import com.aliyun.tablestore.kafka.connect.service.StsService;
import com.aliyun.tablestore.kafka.connect.service.TimeseriesTransformer;
import com.aliyun.tablestore.kafka.connect.utils.ClientUtil;
import com.aliyun.tablestore.kafka.connect.utils.TransformException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @Author lihn
 * @Date 2021/11/26 11:50
 */
public class TimeseriesSinkWriter implements TableStoreSinkWriterInterface{

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesSinkWriter.class);

    private final WriterConfig writerConfig;
    private final String endpoint;
    private final String accessKeyId;
    private final String accessKeySecret;
    private final String instanceName;

    private long clientTimeOutMs;
    private long clientCreateLastTime = System.currentTimeMillis();

    private String regionId;
    private String accountId;
    private String stsAccessId;
    private String stsAccessKey;
    private String roleName;
    private String stsEndpoint;

    private AuthMode authMode;

    private final boolean autoCreate;

    private AsyncTimeseriesClient ots;

    private final TableStoreSinkConfig config;

    private final TimeseriesTransformer transformer;

    private static AtomicLong transformFailedRecords;

    private final RuntimeErrorTolerance tolerance;

    private CredentialsProvider credentialsProvider;

    public TimeseriesSinkWriter(TableStoreSinkConfig config) {
        this.config = config;
        transformer = new TimeseriesTransformer(config);

        writerConfig = createWriterConfig();
        endpoint = config.getString(TableStoreSinkConfig.OTS_ENDPOINT);
        accessKeyId = config.getString(TableStoreSinkConfig.OTS_ACCESS_KEY_ID);
        accessKeySecret = config.getPassword(TableStoreSinkConfig.OTS_ACCESS_KEY_SECRET).value();

        instanceName = config.getString(TableStoreSinkConfig.OTS_INSTANCE_NAME);



        regionId = config.getString(TableStoreSinkConfig.REGION);
        accountId = config.getString(TableStoreSinkConfig.ACCOUNT_ID);
        Map<String, String> env = System.getenv();
        stsAccessId = env.getOrDefault(TableStoreSinkConfig.STS_ACCESS_ID, "");
        stsAccessKey = env.getOrDefault(TableStoreSinkConfig.STS_ACCESS_KEY, "");
        roleName = config.getString(TableStoreSinkConfig.ROLE_NAME);
        stsEndpoint = config.getString(TableStoreSinkConfig.STS_ENDPOINT);
        clientTimeOutMs = config.getLong(TableStoreSinkConfig.CLIENT_TIME_OUT_MS);

        authMode = AuthMode.getType(config.getString(TableStoreSinkConfig.TABLESTORE_AUTH_MODE));
        if (authMode == null) {
            LOGGER.error("auth mode is empty, please check");
            throw new RuntimeException("auth mode is empty, please check");
        }

        ots = InitClient();

        transformFailedRecords = new AtomicLong();
        tolerance = config.getParserErrorTolerance();
        autoCreate = config.getBoolean(TableStoreSinkConfig.AUTO_CREATE);
    }

    @Override
    public void initWriter(Set<String> topics) {
        for (String topic : topics) {

            String tableName = config.getTableNameByTopic(topic);

            LOGGER.info(String.format("Initializing writer for table: %s.", tableName));
            validateOrCreateIfNecessary(tableName);
        }
    }

    @Override
    public boolean needRebuild() {
        return System.currentTimeMillis() - clientCreateLastTime > clientTimeOutMs;
    }

    @Override
    public void rebuildClient() {
        if (authMode == AuthMode.AKSK) {
            // 无需rebuilt
            clientCreateLastTime = System.currentTimeMillis();
            return;
        } else if (authMode == AuthMode.STS) {
            ClientUtil.refreshCredential(accountId, regionId, stsEndpoint, stsAccessId, stsAccessKey, roleName, credentialsProvider);
            clientCreateLastTime = System.currentTimeMillis();
            return;
        }
        return;
    }

    @Override
    public List<ErrantSinkRecord> write(Collection<SinkRecord> records) {

        int requestMaxSize = config.getInt(TableStoreSinkConfig.TIMESERIES_ROWSPERBATCH);

        List<ErrantSinkRecord> errantRecords = new LinkedList<>();//用来存放写入失败的sinkRecord

        Map<String, List<SinkRecord>> usedRecordsMap = new HashMap<>();
        List<Future<PutTimeseriesDataResponse>> futures = new ArrayList<>();
        Map<String, PutTimeseriesDataRequest> requestMap = new HashMap<>();
        // 通过future找到对应record
        Map<Future<PutTimeseriesDataResponse>, List<SinkRecord>> future2RecordListMap = new HashMap<>();

        for (SinkRecord record : records) {
            String topic = record.topic();
            try {
                String tableName = config.getTableNameByTopic(topic);
                PutTimeseriesDataRequest request = requestMap.computeIfAbsent(tableName, k -> new PutTimeseriesDataRequest(k));
                TimeseriesRow row = transformer.transform(tableName, record);
                request.addRow(row);

                List<SinkRecord> tmpRecordList = usedRecordsMap.computeIfAbsent(tableName, k -> new ArrayList<>());
                tmpRecordList.add(record);

                if (request.getRows().size() >= requestMaxSize) {
                    Future<PutTimeseriesDataResponse> future = ots.putTimeseriesData(request, null);
                    futures.add(future);
                    future2RecordListMap.put(future, usedRecordsMap.get(tableName));

                    usedRecordsMap.put(tableName, new ArrayList<>());
                    requestMap.put(tableName, new PutTimeseriesDataRequest(tableName));
                }
            } catch (TransformException | EventParsingException | ConfigException e) {
                LOGGER.debug(String.format("Failed to transform sink record.Total transform failed: %d", transformFailedRecords.incrementAndGet()));
                if (RuntimeErrorTolerance.NONE.equals(tolerance)) {
                    LOGGER.error("An error occurred while converting SinkRecord to RowChange.", e);
                    throw new ConnectException(e);
                } else {
                    errantRecords.add(new ErrantSinkRecord(record, e));
                }
            }
        }


        for (Map.Entry<String, PutTimeseriesDataRequest> entry: requestMap.entrySet()) {
            String tableName = entry.getKey();
            PutTimeseriesDataRequest request = entry.getValue();
            if (request.getRows().size() > 0) {
                Future<PutTimeseriesDataResponse> future = ots.putTimeseriesData(request, null);
                futures.add(future);
                future2RecordListMap.put(future, usedRecordsMap.get(tableName));
            }
        }


        for (int i = 0; i < futures.size(); ++i) {
            try {
                PutTimeseriesDataResponse result = futures.get(i).get();
                if (!result.isAllSuccess()) {
                    List<PutTimeseriesDataResponse.FailedRowResult> failedRows = result.getFailedRows();

                    if (LOGGER.isDebugEnabled()) {
                        for (int k = 0; k < failedRows.size(); k++) {
                            LOGGER.debug(String.format("Failed to write sink record to TableStore.Failed row%s :%s", k, result.getFailedRows().get(k)));
                        }
                    }

                    if (RuntimeErrorTolerance.NONE.equals(tolerance)) {
                        String exceptionStr = "";
                        for (PutTimeseriesDataResponse.FailedRowResult item : failedRows) {
                            LOGGER.error("An error occurred while converting SinkRecord to RowChange.", item.getError());
                            exceptionStr = exceptionStr + item.getError().toString() + ";";
                        }
                        throw new ConnectException(exceptionStr);
                    } else {
                        List<SinkRecord> recordList = future2RecordListMap.get(futures.get(i));
                        for (int j = 0; j < failedRows.size(); j++) {
                            int index = failedRows.get(j).getIndex();
                            errantRecords.add(new ErrantSinkRecord(recordList.get(index), new RuntimeException(failedRows.get(j).getError().toString())));
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("An error occurred while writing.", e);

                if (RuntimeErrorTolerance.NONE.equals(tolerance)) {
                    List<SinkRecord> recordList = future2RecordListMap.get(futures.get(i));
                    String exceptionStr = "An error occurred while converting SinkRecord to RowChange.";
                    for (SinkRecord item : recordList) {
                        LOGGER.error("An error occurred while converting SinkRecord to RowChange.record:" + item.toString());
                        exceptionStr = exceptionStr + item.toString() + ";";
                    }
                    throw new ConnectException(exceptionStr);
                } else {
                    List<SinkRecord> recordList = future2RecordListMap.get(futures.get(i));
                    for (int j = 0; j < recordList.size(); j++) {
                        errantRecords.add(new ErrantSinkRecord(recordList.get(j), new RuntimeException("Error while write to ots")));
                    }
                }
            }
        }

        return errantRecords;
    }

    @Override
    public void closeWriters() {
        ots.shutdown();
    }

    @Override
    public void close() {
        ots.shutdown();
    }


    private AsyncTimeseriesClient InitClient() {
        ClientConfiguration cc = ClientUtil.getClientConfiguration(writerConfig);
        if (authMode == AuthMode.STS) {
            StsUserBo stsUserBo = StsService.getAssumeRole(accountId, regionId, stsEndpoint, stsAccessId, stsAccessKey, roleName);
            credentialsProvider = CredentialsProviderFactory.newDefaultCredentialProvider(stsUserBo.getAk(), stsUserBo.getSk(), stsUserBo.getToken());
            return new AsyncTimeseriesClient(endpoint, credentialsProvider, instanceName, cc, new ResourceManager(cc, null));
        } else {
            // 普通aksk认证，直接构建客户端
            return new AsyncTimeseriesClient(endpoint, accessKeyId, accessKeySecret, instanceName, cc, null);
        }
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

    private void validateOrCreateIfNecessary(String tableName) {
        DescribeTimeseriesTableRequest request = new DescribeTimeseriesTableRequest(tableName);

        int maxRetry = 20;
        while (maxRetry > 0) {
            Future<DescribeTimeseriesTableResponse> result = ots.describeTimeseriesTable(request, null);
            try {
                DescribeTimeseriesTableResponse res = result.get();
                res.getTimeseriesTableMeta();
                break;
            } catch (TableStoreException e) {
                if (e.getHttpStatus() == 500) {
                    if (autoCreate) {
                        try {
                            tryCreateTable(tableName);
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
    }

    private void tryCreateTable(String tableName) {
        TimeseriesTableMeta meta = new TimeseriesTableMeta(tableName);
        meta.getTimeseriesTableOptions().setTimeToLive(-1);
        CreateTimeseriesTableRequest t = new CreateTimeseriesTableRequest(meta);

        Future<CreateTimeseriesTableResponse> future = ots.createTimeseriesTable(t, null);
        try {
            future.get();
            LOGGER.info(String.format("create timeseries table:%s", tableName));
        } catch (InterruptedException e) {
            LOGGER.error("Error while create timeseries table", e);
        } catch (ExecutionException e) {
            LOGGER.error("Error while create timeseries table", e);
        }
    }

}
