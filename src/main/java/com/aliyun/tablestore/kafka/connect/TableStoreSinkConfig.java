package com.aliyun.tablestore.kafka.connect;

import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.enums.*;
import com.aliyun.tablestore.kafka.connect.parsers.EventParser;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TableStoreSinkConfig extends AbstractConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableStoreSinkConfig.class);

    private static final String DELIMITER = ",";


    public static final String STS_ACCESS_ID = "ACCESS_ID";
    public static final String STS_ACCESS_KEY = "ACCESS_KEY";

    public static final String REGION = "region";
    public static final String ACCOUNT_ID = "account.id";
    public static final String ROLE_NAME = "role.name";
    public static final String STS_ENDPOINT = "sts.endpoint";
    public static final String CLIENT_TIME_OUT_MS = "client.time.out.ms";
    /**
     * 配置参数分组
     */
    private static final String KAFKA_GROUP = "Kafka";
    private static final String TABLESTORE_GROUP = "Tablestore";
    private static final String TIMESERIES_GROUP = "Timeseries";
    private static final String CONNECTION_GROUP = "Connection";
    private static final String DATA_MAPPING_GROUP = "Data Mapping";
    private static final String WRITE_GROUP = "Write";
    private static final String ERROR_GROUP = "Error";
    private static final String STS_GROUP = "Sts";


    public static final String CONNECTOR_NAME = "name";
    /**
     * Kafka 主题列表
     */
    public static final String TOPIC_LIST = "topics";


    /**
     * Kafka 消息解析器，默认为 DefaultEventParser
     */
    public static final String EVENT_PARSER = "event.parse.class";
    public static final String EVENT_PARSER_DEFAULT = "com.aliyun.tablestore.kafka.connect.parsers.DefaultEventParser";

    /**
     * OTS 连接相关配置变量
     */
    public static final String OTS_ENDPOINT = "tablestore.endpoint";
    public static final String OTS_ACCESS_KEY_ID = "tablestore.access.key.id";
    public static final String OTS_ACCESS_KEY_SECRET = "tablestore.access.key.secret";
    public static final String OTS_INSTANCE_NAME = "tablestore.instance.name";

    /**
     * 目标表名称的格式字符串，其中包含“<topic>”作为原始 topic 的占位符
     * e.g. kafka_<topic> ,主题 'orders' 将映射到表名 'kafka_orders'
     * topics.assign.tables 配置的优先级更高，若配置了 topics.assign.tables，则忽略 table.name.format的配置
     */
    public static final String TABLE_NAME_FORMAT = "table.name.format";
    public static final String TABLE_NAME_FORMAT_DEFAULT = "<topic>";

    /**
     * topic 和 OTS 表映射变量
     * 以"<topic>:<tablename>"格式映射 topic和表名,topic和表名之间的分隔符为 ":",不同映射之间分隔符为 ","
     * 如果缺省，则采取 table.name.format 的配置
     * 1.主键模式为 kafka时，多个 topic 映射一个 tablename
     * 2.主键模式为 record_key 和 record_value,一个 topic 映射一个 tablename
     * 可以通过 getTableNameByTopic(String topic)获取指定 topic 的 OTS 表名
     * 可以通过 getTableNameList() 获取 OTS 表名列表
     */
    public static final String TOPIC_ASSIGN_TABLE = "topics.assign.tables";
    public static final String TOPIC_ASSIGN_TABLE_DELIMITER = ":";
    public static final String TOPIC_ASSIGN_TABLE_DEFAULT = "";

    /**
     * 主键模式变量，可选 kafka，record_key 或 record_value
     */
    public static final String PRIMARY_KEY_MODE = "primarykey.mode";
    public static final String PRIMARY_KEY_MODE_DEFAULT = "kafka";

    /**
     * 定义 不同 OTS 表主键列配置变量
     * e.g test表的主键列名配置参数为 tablestore.test.primarykey.name，主键列数据类型配置参数为 tablestore.test.primarykey.type
     * 可以通过 getPrimaryKeySchemaListByTable(String tableName) 获取指定表名的 PrimaryKeySchema
     * 主键模式为 kafka 时，主键列名必须为{"topic_partition","offset"}，主键列数据类型必须为{string, integer}，用户配置无法覆盖
     */
    public static final String PRIMARY_KEY_NAME_TEMPLATE = "tablestore.%s.primarykey.name";
    public static final String PRIMARY_KEY_TYPE_TEMPLATE = "tablestore.%s.primarykey.type";
    public static final String PRIMARY_KEY_NAME_TOPIC_PARTITION = "topic_partition";
    public static final String PRIMARY_KEY_NAME_OFFSET = "offset";
    public static final String PRIMARY_KEY_TYPE_TOPIC_PARTITION = "string";
    public static final String PRIMARY_KEY_TYPE_OFFSET = "integer";


    public static final String SEARCH_KEY_MD5 = "md5";
    public static final String SEARCH_KEY_TOPIC = "topic";
    public static final String SEARCH_KEY_PARTITION = "partition";
    public static final String SEARCH_KEY_OFFSET = "offset";
    public static final String SEARCH_FIELD_TIMESTAMP = "timestamp";

    public static final String SEARCH_TIMESTAMP_MODE_TEMPLATE = "tablestore.%s.search.timestamp.type";

    /**
     * 定义 不同 OTS 表过滤属性列配置变量，如果缺省，将写入所有属性列
     * e.g test表的过滤属性列名配置参数为 tablestore.test.columns.whitelist.name，过滤属性列数据类型配置参数为 tablestore.test.columns.whitelist.type
     * 可以通过 getColumnSchemaListByTable(String tableName) 获取指定表名白名单中的 ColumnsSchema
     */
    public static final String COLUMNS_WHITELIST_NAME_TEMPLATE = "tablestore.%s.columns.whitelist.name";
    public static final String COLUMNS_WHITELIST_TYPE_TEMPLATE = "tablestore.%s.columns.whitelist.type";

    /**
     * 写入模式变量，可选 put 或 update，默认为 put
     */
    public static final String INSERT_MODE = "insert.mode";
    public static final String INSERT_MODE_DEFAULT = "put";

    /**
     * 写入 OTS 表是否保序，默认为 true
     */
    public static final String INSERT_ORDER_ENABLE = "insert.order.enable";
    public static final boolean INSERT_ORDER_ENABLE_DEFAULT = true;

    /**
     * 是否自动创建目标表，默认为 false
     */
    public static final String AUTO_CREATE = "auto.create";
    public static final boolean AUTO_CREATE_DEFAULT = false;

    /**
     * 是否可以支持删除模式，默认为 none
     */
    public static final String DELETE_MODE = "delete.mode";
    public static final String DELETE_ENABLE_DEFAULT = "none";

    /**
     * 导入 OTS 表时内存中缓冲队列的大小，默认为 1024
     */
    public static final String BUFFER_SIZE = "buffer.size";
    public static final int BUFFER_SIZE_DEFAULT = 1024;

    /**
     * 写入进程的回调处理线程数，默认核数+1
     */
    public static final String MAX_THREAD_COUNT = "max.thread.count";
    public static final int MAX_THREAD_COUNT_DEFAULT = Runtime.getRuntime().availableProcessors() + 1;

    /**
     * 导入 OTS 表时的最大请求并发数，默认为 10
     */
    public static final String MAX_CONCURRENCY = "max.concurrency";
    public static final int MAX_CONCURRENCY_DEFAULT = 10;

    /**
     * 导入 OTS 表时的分桶数，适当调大可提升并发写入能力，但不应大于并发数，默认为 3
     */
    public static final String BUCKET_COUNT = "bucket.count";
    public static final int BUCKET_COUNT_DEFAULT = 3;

    /**
     * 写入 OTS 表时对缓冲区的刷新时间间隔（单位：ms），默认为 10000
     */
    public static final String FLUSH_INTERVAL = "flush.Interval";
    public static final int FLUSH_INTERVAL_DEFAULT = 10000;


    /**
     * 当 SinkRecord 解析成 RowChange 产生错误时，对该SinkRecord 可选两种处理：none, all
     */
    public static final String RUNTIME_ERROR_TOLERANCE = "runtime.error.tolerance";
    public static final String RUNTIME_ERROR_TOLERANCE_DEFAULT = "none";


    /**
     * 运行错误处理方式（SinkRecord 解析错误或写入OTS时产生错误）
     */
    public static final String RUNTIME_ERROR_MODE = "runtime.error.mode";
    public static final String RUNTIME_ERROR_MODE_DEFAULT = "ignore";

    /**
     * 运行错误处理方式选择 kafka 时，存储脏数据的集群和topic
     */
    public static final String RUNTIME_ERROR_BOOTSTRAP_SERVERS = "runtime.error.bootstrap.servers";
    public static final String RUNTIME_ERROR_BOOTSTRAP_SERVERS_DEFAULT = "";
    public static final String RUNTIME_ERROR_TOPIC_NAME = "runtime.error.topic.name";
    public static final String RUNTIME_ERROR_TOPIC_NAME_DEFAULT = "";

    /**
     * 运行错误处理方式选择 kafka 时，存储脏数据的集群和topic
     */
    public static final String RUNTIME_ERROR_TABLE_NAME = "runtime.error.table.name";
    public static final String RUNTIME_ERROR_TABLE_NAME_DEFAULT = "";


    public static final int CLIENT_RETRY_TIME_SECONDS = 60;

    /**
     *
     */
    public static final String TABLESTORE_MODE = "tablestore.mode";
    public static final String TABLESTORE_MODE_DEFAULT = "normal";
    public static final String TABLESTORE_AUTH_MODE = "tablestore.auth.mode";
    public static final String TABLESTORE_AUTH_MODE_DEFAULT = "STS";
    public static final String TIMESERIES_MNAME = "tablestore.timeseries.%s.measurement";
    public static final String TIMESERIES_DATASOURCE = "tablestore.timeseries.%s.dataSource";
    public static final String TIMESERIES_TAGS = "tablestore.timeseries.%s.tags";
    public static final String TIMESERIES_TIME = "tablestore.timeseries.%s.time";
    public static final String TIMESERIES_TIME_UNIT = "tablestore.timeseries.%s.time.unit";
    public static final String TIMESERIES_FIELDS_NAME = "tablestore.timeseries.%s.field.name";
    public static final String TIMESERIES_FIELDS_TYPE = "tablestore.timeseries.%s.field.type";
    public static final String TIMESERIES_MAPALL = "tablestore.timeseries.mapAll";
    public static final String TIMESERIES_TO_LOWERCASE = "tablestore.timeseries.toLowerCase";

    public static final String TIMESERIES_ROWSPERBATCH = "tablestore.timeseries.rowsPerBatch";
    public static final int TIMESERIES_ROWSPERBATCH_DEFAULT = 200;

    public static final String TIMESERIES_MNAME_TOPIC = "<topic>";


    /**
     * 配置定义
     */
    public static final ConfigDef CONFIG_DEF;

    static {
        CONFIG_DEF = new ConfigDef()
                .define(TOPIC_LIST,
                        ConfigDef.Type.LIST,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "The topic list of Kafka SinkRecord.",
                        KAFKA_GROUP,
                        1,
                        ConfigDef.Width.LONG,
                        "Topic List"
                )
                .define(CONNECTOR_NAME,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "Connector Name.",
                        KAFKA_GROUP,
                        2,
                        ConfigDef.Width.LONG,
                        "Connector Name"
                )
                .define(TABLESTORE_MODE,
                        ConfigDef.Type.STRING,
                        TABLESTORE_MODE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Tablestore connector working mode",
                        TABLESTORE_GROUP,
                        1,
                        ConfigDef.Width.MEDIUM,
                        "working mode")
                .define(TABLESTORE_AUTH_MODE,
                        ConfigDef.Type.STRING,
                        TABLESTORE_AUTH_MODE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        "Tablestore auth mode",
                        TABLESTORE_GROUP,
                        2,
                        ConfigDef.Width.MEDIUM,
                        "auth mode")
                .define(TIMESERIES_MAPALL,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.MEDIUM,
                        "auto mapping the key to ots fields",
                        TIMESERIES_GROUP,
                        1,
                        ConfigDef.Width.MEDIUM,
                        "mapAll")
                .define(TIMESERIES_TO_LOWERCASE,
                        ConfigDef.Type.BOOLEAN,
                        true,
                        ConfigDef.Importance.MEDIUM,
                        "transfer the not-primary key to lower case when save to ots",
                        TIMESERIES_GROUP,
                        2,
                        ConfigDef.Width.MEDIUM,
                        "timeseries_tolowercase")
                .define(TIMESERIES_ROWSPERBATCH,
                        ConfigDef.Type.INT,
                        TIMESERIES_ROWSPERBATCH_DEFAULT,
                        ConfigDef.Importance.LOW,
                        "rows sent per batch when write to ots",
                        TIMESERIES_GROUP,
                        3,
                        ConfigDef.Width.MEDIUM,
                        "timeseries_rowPerBatch")
                .define(OTS_ENDPOINT,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "The domain name of the OTS instance.",
                        CONNECTION_GROUP,
                        1,
                        ConfigDef.Width.LONG,
                        "OTS Endpoint")
                .define(OTS_ACCESS_KEY_ID,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.HIGH,
                        "AccessKey ID for OTS.",
                        CONNECTION_GROUP,
                        2,
                        ConfigDef.Width.MEDIUM,
                        "AccessKey ID")
                .define(OTS_ACCESS_KEY_SECRET,
                        ConfigDef.Type.PASSWORD,
                        "",
                        ConfigDef.Importance.HIGH,
                        "AccessKey Secret for OTS.",
                        CONNECTION_GROUP,
                        3,
                        ConfigDef.Width.MEDIUM,
                        "AccessKey Secret")
                .define(OTS_INSTANCE_NAME,
                        ConfigDef.Type.STRING,
                        ConfigDef.NO_DEFAULT_VALUE,
                        ConfigDef.Importance.HIGH,
                        "OTS instance name.",
                        CONNECTION_GROUP,
                        4,
                        ConfigDef.Width.MEDIUM,
                        "OTS Instance")
                .define(EVENT_PARSER,
                        ConfigDef.Type.CLASS,
                        EVENT_PARSER_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        "The parser used for Kafka SinkRecord.",
                        DATA_MAPPING_GROUP,
                        1,
                        ConfigDef.Width.LONG,
                        "SinkRecord Parser"
                )
                .define(TABLE_NAME_FORMAT,
                        ConfigDef.Type.STRING,
                        TABLE_NAME_FORMAT_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "The format string of the destination table name.",
                        DATA_MAPPING_GROUP,
                        2,
                        ConfigDef.Width.MEDIUM,
                        "Table name format")
                .define(TOPIC_ASSIGN_TABLE,
                        ConfigDef.Type.LIST,
                        TOPIC_ASSIGN_TABLE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "The mapping between topics and table names.",
                        DATA_MAPPING_GROUP,
                        3,
                        ConfigDef.Width.LONG,
                        "Topics Assign Tables")
                .define(PRIMARY_KEY_MODE,
                        ConfigDef.Type.STRING,
                        PRIMARY_KEY_MODE_DEFAULT,
                        EnumValidator.in(PrimaryKeyMode.values()),
                        ConfigDef.Importance.HIGH,
                        "The mode of OTS primaryKey.Supported modes are: kafka, record_key and record_value.",
                        DATA_MAPPING_GROUP,
                        4,
                        ConfigDef.Width.SHORT,
                        "PrimaryKey Mode")
                .define(INSERT_MODE,
                        ConfigDef.Type.STRING,
                        INSERT_MODE_DEFAULT,
                        EnumValidator.in(InsertMode.values()),
                        ConfigDef.Importance.HIGH,
                        "The insertion mode to use. Supported modes are: put and update.",
                        WRITE_GROUP,
                        1,
                        ConfigDef.Width.SHORT,
                        "Insert Mode")
                .define(INSERT_ORDER_ENABLE,
                        ConfigDef.Type.BOOLEAN,
                        INSERT_ORDER_ENABLE_DEFAULT,
                        ConfigDef.Importance.HIGH,
                        "Whether to keep the write order.",
                        WRITE_GROUP,
                        2,
                        ConfigDef.Width.SHORT,
                        "Enable Insert Order")
                .define(AUTO_CREATE,
                        ConfigDef.Type.BOOLEAN,
                        AUTO_CREATE_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Whether to automatically create the destination table.",
                        WRITE_GROUP,
                        3,
                        ConfigDef.Width.SHORT,
                        "Auto create")
                .define(DELETE_MODE,
                        ConfigDef.Type.STRING,
                        DELETE_ENABLE_DEFAULT,
                        EnumValidator.in(DeleteMode.values()),
                        ConfigDef.Importance.MEDIUM,
                        "The delete mode to use. Supported modes are: none, row, column and row_and_column.",
                        WRITE_GROUP,
                        4,
                        ConfigDef.Width.SHORT,
                        "Delete Mode")
                .define(BUFFER_SIZE,
                        ConfigDef.Type.INT,
                        BUFFER_SIZE_DEFAULT,
                        PowerOfTwoValidator.create(),
                        ConfigDef.Importance.MEDIUM,
                        "Specifies how many rows are written to the local buffer queue.",
                        WRITE_GROUP,
                        5,
                        ConfigDef.Width.SHORT,
                        "Buffer Size")
                .define(MAX_THREAD_COUNT,
                        ConfigDef.Type.INT,
                        MAX_THREAD_COUNT_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        "Specifies the maximum number of callback threads for writing processes",
                        WRITE_GROUP,
                        6,
                        ConfigDef.Width.SHORT,
                        "Max Thread Count")
                .define(MAX_CONCURRENCY,
                        ConfigDef.Type.INT,
                        MAX_CONCURRENCY_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        "Specifies the maximum number of concurrent write threads.",
                        WRITE_GROUP,
                        7,
                        ConfigDef.Width.SHORT,
                        "Max Concurrency")
                .define(BUCKET_COUNT,
                        ConfigDef.Type.INT,
                        BUCKET_COUNT_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.MEDIUM,
                        "Specifies the number of buckets.",
                        WRITE_GROUP,
                        8,
                        ConfigDef.Width.SHORT,
                        "Bucket Count")
                .define(FLUSH_INTERVAL,
                        ConfigDef.Type.INT,
                        FLUSH_INTERVAL_DEFAULT,
                        ConfigDef.Range.atLeast(1),
                        ConfigDef.Importance.LOW,
                        "Specifies the buffer refresh time interval.",
                        WRITE_GROUP,
                        9,
                        ConfigDef.Width.SHORT,
                        "Flush Interval")
                .define(RUNTIME_ERROR_TOLERANCE,
                        ConfigDef.Type.STRING,
                        RUNTIME_ERROR_TOLERANCE_DEFAULT,
                        EnumValidator.in(RuntimeErrorTolerance.values()),
                        ConfigDef.Importance.MEDIUM,
                        "Error tolerance during parsing.",
                        ERROR_GROUP,
                        1,
                        ConfigDef.Width.SHORT,
                        "Parse error tolerance")
                .define(RUNTIME_ERROR_MODE,
                        ConfigDef.Type.STRING,
                        RUNTIME_ERROR_MODE_DEFAULT,
                        EnumValidator.in(RunTimeErrorMode.values()),
                        ConfigDef.Importance.MEDIUM,
                        "The processing mode of runtime errors.",
                        ERROR_GROUP,
                        2,
                        ConfigDef.Width.SHORT,
                        "Runtime error mode")
                .define(RUNTIME_ERROR_BOOTSTRAP_SERVERS,
                        ConfigDef.Type.STRING,
                        RUNTIME_ERROR_BOOTSTRAP_SERVERS_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Kafka cluster for storing running errors.",
                        ERROR_GROUP,
                        3,
                        ConfigDef.Width.LONG,
                        "Runtime error bootstrap servers")
                .define(RUNTIME_ERROR_TOPIC_NAME,
                        ConfigDef.Type.STRING,
                        RUNTIME_ERROR_TOPIC_NAME_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Kafka topic for storing running errors.",
                        ERROR_GROUP,
                        4,
                        ConfigDef.Width.MEDIUM,
                        "Runtime error topic")
                .define(RUNTIME_ERROR_TABLE_NAME,
                        ConfigDef.Type.STRING,
                        RUNTIME_ERROR_TABLE_NAME_DEFAULT,
                        ConfigDef.Importance.MEDIUM,
                        "Table name for storing running errors.",
                        ERROR_GROUP,
                        5,
                        ConfigDef.Width.MEDIUM,
                        "Runtime error table")
                .define(REGION,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        "region.",
                        STS_GROUP,
                        1,
                        ConfigDef.Width.MEDIUM,
                        "Runtime error region")
                .define(ACCOUNT_ID,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        "accountid.",
                        STS_GROUP,
                        2,
                        ConfigDef.Width.MEDIUM,
                        "Runtime error accountid")
                .define(ROLE_NAME,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        "rolename.",
                        STS_GROUP,
                        3,
                        ConfigDef.Width.MEDIUM,
                        "Runtime error rolename")
                .define(STS_ENDPOINT,
                        ConfigDef.Type.STRING,
                        "",
                        ConfigDef.Importance.MEDIUM,
                        "sts end point.",
                        STS_GROUP,
                        4,
                        ConfigDef.Width.MEDIUM,
                        "sts end point")
                .define(CLIENT_TIME_OUT_MS,
                        ConfigDef.Type.LONG,
                        1000 * 60 * 60,
                        ConfigDef.Importance.MEDIUM,
                        "sts time out.",
                        STS_GROUP,
                        5,
                        ConfigDef.Width.MEDIUM,
                        "sts time out");
    }


    private final EventParser parser;
    private final PrimaryKeyMode primaryKeyMode;
    private final InsertMode insertMode;
    private final DeleteMode deleteMode;
    private final TablestoreMode tablestoreMode;
    private final RuntimeErrorTolerance parserErrorTolerance;
    private final RunTimeErrorMode runTimeErrorMode;
    private final List<String> tableNameList;
    private final Map<String, String> tableNameByTopic;
    private final Map<String, List<PrimaryKeySchema>> primaryKeySchemaByTable;
    private final Map<String, List<DefinedColumnSchema>> whitelistColumnSchemaByTable;
    private final Map<String, Map<String, DefinedColumnSchema>> timeSeriesFieldName2ColumnSchemaByTable;

    private final Map<String, Set<String>> timeseriesKeys = new ConcurrentHashMap<>();
    private final Map<String, SearchTimeMode> searchTimeModeMap = new HashMap<>();

    private final boolean timeseriesToLowerCase;
    private final boolean timeseriesMapAll;

    public TableStoreSinkConfig(Map<String, String> originals) {
        this(CONFIG_DEF, originals);
    }

    public TableStoreSinkConfig(ConfigDef config, Map<String, String> originals) {
        super(config, originals, false);

        primaryKeyMode = PrimaryKeyMode.valueOf(getString(PRIMARY_KEY_MODE).toUpperCase());
        insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
        deleteMode = DeleteMode.valueOf(getString(DELETE_MODE).toUpperCase());

        tablestoreMode = TablestoreMode.getType(getString(TableStoreSinkConfig.TABLESTORE_MODE));

        if (!DeleteMode.NONE.equals(deleteMode) && primaryKeyMode != PrimaryKeyMode.RECORD_KEY) {
            throw new ConfigException(
                    "Primary key mode must be 'record_key' when delete support is enabled");
        }

        parserErrorTolerance = RuntimeErrorTolerance.valueOf(getString(RUNTIME_ERROR_TOLERANCE).toUpperCase());
        runTimeErrorMode = RunTimeErrorMode.valueOf(getString(RUNTIME_ERROR_MODE).toUpperCase());

        if (RunTimeErrorMode.KAFKA.equals(runTimeErrorMode)) {
            if (RUNTIME_ERROR_BOOTSTRAP_SERVERS_DEFAULT.equals(getString(RUNTIME_ERROR_BOOTSTRAP_SERVERS))
                    || RUNTIME_ERROR_TOPIC_NAME_DEFAULT.equals(getString(RUNTIME_ERROR_TOPIC_NAME))) {
                throw new ConfigException(
                        "Kafka bootstrap servers and topic cannot be null when runtime error mode is KAFKA");
            }
        }

        if (RunTimeErrorMode.TABLESTORE.equals(runTimeErrorMode)) {
            if (RUNTIME_ERROR_TABLE_NAME_DEFAULT.equals(getString(RUNTIME_ERROR_TABLE_NAME))) {
                throw new ConfigException(
                        "Table name cannot be null when runtime error mode is TABLESTORE");
            }
        }

        parser = createParser();

        tableNameList = new ArrayList<>();
        tableNameByTopic = createTableNameByTopic();

        primaryKeySchemaByTable = createPrimaryKeySchemaList();
        whitelistColumnSchemaByTable = createColumnSchemaList();

        timeSeriesFieldName2ColumnSchemaByTable = createTimeseriesColumnSchemaList();
        timeseriesToLowerCase = getBoolean(TIMESERIES_TO_LOWERCASE);
        timeseriesMapAll = getBoolean(TIMESERIES_MAPALL);

        if (tablestoreMode == TablestoreMode.TIMESERIES && !timeseriesMapAll && (timeSeriesFieldName2ColumnSchemaByTable == null || timeSeriesFieldName2ColumnSchemaByTable.isEmpty())) {
            throw new ConfigException("map all can not be false while timeSeriesColumnSchemaByTable is empty.");
        }

    }


    private Map<String, Map<String, DefinedColumnSchema>> createTimeseriesColumnSchemaList() {
        if (tablestoreMode != TablestoreMode.TIMESERIES) {
            return new HashMap<>();
        }

        Map<String, Map<String, DefinedColumnSchema>> result = new HashMap<>();
        for (String tableName : tableNameList) {
            Map<String, DefinedColumnSchema> columnSchemas = new HashMap<>();
            String colNameString = originalsStrings().getOrDefault(String.format(TIMESERIES_FIELDS_NAME, tableName), "");
            String colTypeString = originalsStrings().getOrDefault(String.format(TIMESERIES_FIELDS_TYPE, tableName), "");

            if (colNameString == null || colNameString.isEmpty()) {
                continue;
            }

            String[] colNameList = colNameString.split(DELIMITER);
            //1.检查属性列是否为空，是否重复
            Set<String> set=new HashSet<>();
            for(String colName:colNameList){
                if(colName.length()==0){
                    throw new ConfigException("The attribute column name in timeseries field cannot be an empty string.");
                }
                if(set.contains(colName)){
                    throw new ConfigException("The attribute column name in timeseries field must be unique.");
                }
                set.add(colName);
            }

            String[] colTypeList = colTypeString.split(DELIMITER);

            //1.没有配置属性列 name，type配置无效，视为没有配置白名单
            if (colNameList.length == 0) {
                result.put(tableName, columnSchemas);
                continue;
            }

            //2.检查 name和 type 个数是否一致
            if (colNameList.length != colTypeList.length) {
                throw new ConfigException(String.format(TIMESERIES_FIELDS_NAME, tableName)
                        + " : " + colTypeString
                        + " does not match "
                        + String.format(TIMESERIES_FIELDS_TYPE, tableName)
                        + " : " + colNameString);
            }


            EnumValidator colTypeValidator = EnumValidator.in(DefinedColumnType.values());
            for (int i = 0; i < colNameList.length; ++i) {
                colTypeValidator.ensureValid(String.format(TIMESERIES_FIELDS_TYPE, tableName), colTypeList[i].trim());
                columnSchemas.put(colNameList[i].trim().toLowerCase(),
                        new DefinedColumnSchema(colNameList[i].trim(), DefinedColumnType.valueOf(colTypeList[i].trim().toUpperCase()))
                );
            }
            result.put(tableName, columnSchemas);
        }

        if (!result.isEmpty()) {
            LOGGER.info(String.format("timeseriesColumnSchema config: %s", result));
        } else {
            LOGGER.info("timeseriesColumnSchema config is empty");
        }

        return result;
    }

    /**
     * 实例化 Kafka 消息解析器
     *
     * @return EventParser
     */
    private EventParser createParser() {
        EventParser parser;
        try {
            parser = (EventParser) getClass(EVENT_PARSER).newInstance();
        } catch (IllegalAccessException | InstantiationException e) {
            throw new RuntimeException(e);
        }
        return parser;
    }

    /**
     * 建立 Kafka Topic 和 OTS 表名的映射关系
     */
    private Map<String, String> createTableNameByTopic() {
        Map<String, String> tableNameByTopic = new HashMap<>();
        Set<String> tableNameSet = new HashSet<>();
        List<String> assignList = getList(TOPIC_ASSIGN_TABLE);

        for (String assign : assignList) {
            String[] split = assign.split(TOPIC_ASSIGN_TABLE_DELIMITER);
            String topic = split[0].trim(), tableName = split[1].trim();
            tableNameByTopic.put(topic, tableName);
            tableNameSet.add(tableName);
        }

        List<String> topicList = getList(TOPIC_LIST);
        for (String topic : topicList) {
            if (!tableNameByTopic.containsKey(topic)) {
                String tableName = getString(TABLE_NAME_FORMAT).replace("<topic>", topic);
                tableNameByTopic.put(topic, tableName);
                tableNameSet.add(tableName);
            }
        }
        this.tableNameList.addAll(tableNameSet);
        return tableNameByTopic;
    }

    public Set<String> getAllTopics() {
        if (this.tableNameByTopic == null || this.tableNameByTopic.size() == 0) {
            return new HashSet<>();
        }
        return this.tableNameByTopic.keySet();
    }

    public String getName() {
        return getString(CONNECTOR_NAME);
    }

    public boolean toLowerCaseTimeseries() {
        return timeseriesToLowerCase;
    }

    public boolean timeseriesMapAll() {
        return timeseriesMapAll;
    }

    /**
     * 建立 OTS 表名 和 PrimaryKeySchemaList 的映射关系
     */
    private Map<String, List<PrimaryKeySchema>> createPrimaryKeySchemaList() {
        Map<String, List<PrimaryKeySchema>> primaryKeySchemaByTable = new HashMap<>();
        for (String tableName : tableNameList) {
            List<PrimaryKeySchema> pkSchemaList = new ArrayList<>();
            if (primaryKeyMode.equals(PrimaryKeyMode.KAFKA)) {
                pkSchemaList.add(new PrimaryKeySchema(PRIMARY_KEY_NAME_TOPIC_PARTITION, PrimaryKeyType.valueOf(PRIMARY_KEY_TYPE_TOPIC_PARTITION.toUpperCase())));
                pkSchemaList.add(new PrimaryKeySchema(PRIMARY_KEY_NAME_OFFSET, PrimaryKeyType.valueOf(PRIMARY_KEY_TYPE_OFFSET.toUpperCase())));
            } else if (primaryKeyMode.equals(PrimaryKeyMode.SEARCH)) {
                pkSchemaList.add(new PrimaryKeySchema(SEARCH_KEY_MD5, PrimaryKeyType.STRING));
                pkSchemaList.add(new PrimaryKeySchema(SEARCH_KEY_TOPIC, PrimaryKeyType.STRING));
                pkSchemaList.add(new PrimaryKeySchema(SEARCH_KEY_PARTITION, PrimaryKeyType.INTEGER));
                pkSchemaList.add(new PrimaryKeySchema(SEARCH_KEY_OFFSET, PrimaryKeyType.INTEGER));
            } else {
                String pkNameString = originalsStrings().getOrDefault(String.format(PRIMARY_KEY_NAME_TEMPLATE, tableName), DELIMITER);
                String pkTypeString = originalsStrings().getOrDefault(String.format(PRIMARY_KEY_TYPE_TEMPLATE, tableName), DELIMITER);

                String[] pkNameList = pkNameString.split(DELIMITER);

                //1.检查主键是否为空，是否重复
                Set<String> set=new HashSet<>();
                for(String pkName:pkNameList){
                    if(pkName.length()==0){
                        throw new ConfigException("The primary key name cannot be an empty string.");
                    }
                    if(set.contains(pkName)){
                        throw new ConfigException("The primary key name must be unique.");
                    }
                    set.add(pkName);
                }

                String[] pkTypeList = pkTypeString.split(DELIMITER);

                //2.检查是否配置主键列
                if (pkNameList.length == 0) {
                    throw new ConfigException(
                            String.format("The primary key definition is missing and the primary key mode is %s", this.primaryKeyMode));
                }

                //2.检查 name和 type 个数是否一致
                if (pkNameList.length != pkTypeList.length) {
                    throw new ConfigException(String.format(PRIMARY_KEY_NAME_TEMPLATE, tableName)
                            + " : " + pkNameString
                            + " does not match "
                            + String.format(PRIMARY_KEY_TYPE_TEMPLATE, tableName)
                            + " : " + pkTypeString);
                }

                EnumValidator pkTypeValidator = EnumValidator.in(PrimaryKeyType.values());
                for (int i = 0; i < pkNameList.length; ++i) {
                    boolean isAutoIncrement = false;
                    if ("auto_increment".equals(pkTypeList[i])) {
                        isAutoIncrement = true;
                        pkTypeList[i] = "integer";
                    } else {
                        pkTypeValidator.ensureValid(String.format(PRIMARY_KEY_TYPE_TEMPLATE, tableName), pkTypeList[i].trim());
                    }
                    PrimaryKeySchema primaryKeySchema = new PrimaryKeySchema(pkNameList[i].trim(), PrimaryKeyType.valueOf(pkTypeList[i].trim().toUpperCase()));
                    if (isAutoIncrement) {
                        primaryKeySchema.setOption(PrimaryKeyOption.AUTO_INCREMENT);
                    }
                    pkSchemaList.add(primaryKeySchema);
                }

            }
            primaryKeySchemaByTable.put(tableName, pkSchemaList);

        }

        return primaryKeySchemaByTable;
    }

    /**
     * 建立 OTS 表名 和  ColumnSchemaInWhiteList 的映射关系
     */
    private Map<String, List<DefinedColumnSchema>> createColumnSchemaList() {
        Map<String, List<DefinedColumnSchema>> whitelistColumnSchemaByTable = new HashMap<>();
        for (String tableName : tableNameList) {
            List<DefinedColumnSchema> whitelistColumnSchema = new ArrayList<>();
            String colNameString = originalsStrings().getOrDefault(String.format(COLUMNS_WHITELIST_NAME_TEMPLATE, tableName), DELIMITER);
            String colTypeString = originalsStrings().getOrDefault(String.format(COLUMNS_WHITELIST_TYPE_TEMPLATE, tableName), DELIMITER);

            String[] colNameList = colNameString.split(DELIMITER);
            //1.检查属性列是否为空，是否重复
            Set<String> set=new HashSet<>();
            for(String colName:colNameList){
                if(colName.length()==0){
                    throw new ConfigException("The attribute column name in whitelist cannot be an empty string.");
                }
                if(set.contains(colName)){
                    throw new ConfigException("The attribute column name in whitelist must be unique.");
                }
                set.add(colName);
            }

            String[] colTypeList = colTypeString.split(DELIMITER);

            //1.没有配置属性列 name，type配置无效，视为没有配置白名单
            if (colNameList.length == 0) {
                whitelistColumnSchemaByTable.put(tableName, whitelistColumnSchema);
                continue;
            }

            //2.检查 name和 type 个数是否一致
            if (colNameList.length != colTypeList.length) {
                throw new ConfigException(String.format(COLUMNS_WHITELIST_TYPE_TEMPLATE, tableName)
                        + " : " + colTypeString
                        + " does not match "
                        + String.format(COLUMNS_WHITELIST_NAME_TEMPLATE, tableName)
                        + " : " + colNameString);
            }


            EnumValidator colTypeValidator = EnumValidator.in(DefinedColumnType.values());
            for (int i = 0; i < colNameList.length; ++i) {
                colTypeValidator.ensureValid(String.format(COLUMNS_WHITELIST_TYPE_TEMPLATE, tableName), colTypeList[i].trim());
                whitelistColumnSchema.add(
                        new DefinedColumnSchema(colNameList[i].trim(), DefinedColumnType.valueOf(colTypeList[i].trim().toUpperCase()))
                );
            }
            whitelistColumnSchemaByTable.put(tableName, whitelistColumnSchema);
        }
        return whitelistColumnSchemaByTable;
    }

    /**
     * 获取 Kafka SinkRecord 解析器
     *
     * @return EventParser
     */
    public EventParser getParser() {
        return parser;
    }

    /**
     * 获取主键模式
     *
     * @return PrimaryKeyMode
     */
    public PrimaryKeyMode getPrimaryKeyMode() {
        return primaryKeyMode;
    }

    /**
     * 获取写入模式
     *
     * @return InsertMode
     */
    public InsertMode getInsertMode() {
        return insertMode;
    }

    /**
     * 获取删除模式
     *
     * @return DeleteMode
     */
    public DeleteMode getDeleteMode() {
        return deleteMode;
    }





    /**
     * 获取解析容错能力
     *
     * @return ParserErrorTolerance
     */
    public RuntimeErrorTolerance getParserErrorTolerance() {
        return parserErrorTolerance;
    }

    /**
     * 获取运行错误处理模式
     */
    public RunTimeErrorMode getRunTimeErrorMode() {
        return runTimeErrorMode;
    }

    /**
     * 获取指定 topic 的 OTS 表名
     *
     * @return TableName
     */
    public String getTableNameByTopic(String topic) {
        if (!this.tableNameByTopic.containsKey(topic)) {
            throw new ConfigException(
                    String.format("Topic %s cannot be found", topic));
        }
        return this.tableNameByTopic.get(topic);
    }


    public TablestoreMode getTablestoreMode() {
        return tablestoreMode;
    }


    public SearchTimeMode getSearchTimeMode(String tableName) {
        if (!searchTimeModeMap.containsKey(tableName)) {
            String mNameField = originalsStrings().getOrDefault(String.format(SEARCH_TIMESTAMP_MODE_TEMPLATE, tableName), "");
            SearchTimeMode timeMode = SearchTimeMode.getType(mNameField);
            if (timeMode != null) {
                searchTimeModeMap.put(tableName, timeMode);
                LOGGER.info(String.format("search time mode, tableName:%s, mode:%s", tableName, timeMode));
            } else {
                searchTimeModeMap.put(tableName, SearchTimeMode.KAFKA);
                LOGGER.info(String.format("search time mode, tableName:%s, mode:%s", tableName, SearchTimeMode.KAFKA));
            }
        }

        return searchTimeModeMap.get(tableName);
    }


    /**
     * 获取 OTS 表名列表
     *
     * @return 表名列表
     */
    public List<String> getTableNameList() {
        return tableNameList;
    }

    /**
     * 获取指定 tableName 的 PrimaryKeySchema
     *
     * @return PrimaryKeySchema 列表
     */
    public List<PrimaryKeySchema> getPrimaryKeySchemaListByTable(String tableName) {
        if (!this.primaryKeySchemaByTable.containsKey(tableName)) {
            throw new ConfigException(
                    String.format("Table %s cannot be found", tableName));
        }
        return this.primaryKeySchemaByTable.get(tableName);
    }

    /**
     * 获取指定 tableName 的白名单属性列 Schema
     *
     * @return DefinedColumnSchema 列表
     */
    public List<DefinedColumnSchema> getColumnSchemaListByTable(String tableName) {
        if (!this.whitelistColumnSchemaByTable.containsKey(tableName)) {
            throw new ConfigException(
                    String.format("OTS table %s cannot be found", tableName));
        }
        return this.whitelistColumnSchemaByTable.get(tableName);
    }

    public Map<String, DefinedColumnSchema> getTimeseriesColumnSchemaMapByTable(String tableName) {
        return this.timeSeriesFieldName2ColumnSchemaByTable.get(tableName);
    }

    public Set<String> getTimeseriesKeyStrings(String tableName) {
        if (!timeseriesKeys.containsKey(tableName)) {
            Set<String> keyStrings = new HashSet<>();
            String mNameField = originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_MNAME, tableName), "");
            if (!mNameField.equalsIgnoreCase(TableStoreSinkConfig.TIMESERIES_MNAME_TOPIC)) {
                keyStrings.add(originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_MNAME, tableName), ""));
            }
            keyStrings.add(originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_DATASOURCE, tableName), ""));
            keyStrings.add(originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_TIME, tableName), ""));

            String tagValue = originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_TAGS, tableName), "");
            for (String str : tagValue.split(",")) {
                keyStrings.add(str);
            }
            timeseriesKeys.put(tableName, keyStrings);
        }
        return timeseriesKeys.get(tableName);
    }

    /**
     * 枚举类型验证器,不区分大小写
     */
    private static class EnumValidator implements ConfigDef.Validator {
        private final List<String> canonicalValues;
        private final Set<String> validValues;

        private EnumValidator(List<String> canonicalValues, Set<String> validValues) {
            this.canonicalValues = canonicalValues;
            this.validValues = validValues;
        }

        public static <E> EnumValidator in(E[] enumerators) {
            final List<String> canonicalValues = new ArrayList<>(enumerators.length);
            final Set<String> validValues = new HashSet<>(enumerators.length);
            for (E e : enumerators) {
                canonicalValues.add(e.toString().toLowerCase());
                validValues.add(e.toString().toLowerCase());
            }
            return new EnumValidator(canonicalValues, validValues);
        }

        @Override
        public void ensureValid(String key, Object value) {
            if (!validValues.contains(value.toString().toLowerCase())) {
                throw new ConfigException(key, value);
            }
        }

        @Override
        public String toString() {
            return canonicalValues.toString();
        }
    }

    /**
     * 2 的指数验证器
     */
    public static class PowerOfTwoValidator implements ConfigDef.Validator {

        private PowerOfTwoValidator() {
        }

        public static PowerOfTwoValidator create() {

            return new PowerOfTwoValidator();
        }

        @Override
        public void ensureValid(String key, Object value) {
            long n = Long.parseLong(value.toString());
            if (n <= 0) {
                throw new ConfigException(key, value, "The value must be a positive integer.");
            }
            if ((n & (n - 1)) != 0) {
                throw new ConfigException(key, value, "The value must be the power of 2.");
            }
        }
    }
}
