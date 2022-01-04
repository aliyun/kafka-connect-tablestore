package com.aliyun.tablestore.kafka.connect.utils;

import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.enums.DeleteMode;
import com.aliyun.tablestore.kafka.connect.enums.InsertMode;
import com.aliyun.tablestore.kafka.connect.enums.PrimaryKeyMode;
import com.aliyun.tablestore.kafka.connect.parsers.EventParser;
import com.aliyun.tablestore.kafka.connect.parsers.EventParsingException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RowChangeTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RowChangeTransformer.class);

    private TableStoreSinkConfig config;
    private EventParser parser;
    private InsertMode insertMode;
    private DeleteMode deleteMode;
    private PrimaryKeyMode primaryKeyMode;
    private boolean isDeleteRow;

    public RowChangeTransformer(TableStoreSinkConfig config) {
        this.config = config;
        this.parser = config.getParser();
        this.insertMode = config.getInsertMode();
        this.deleteMode = config.getDeleteMode();
        this.primaryKeyMode = config.getPrimaryKeyMode();
        this.isDeleteRow = false;
    }

    /**
     * 将 SinkRecord 转换为 RowChange
     *
     * @param tableName
     * @param sinkRecord
     */
    public RowChange transform(String tableName, SinkRecord sinkRecord) throws EventParsingException, TransformException {

        RowChange rowChange = null;
        try {
            PrimaryKey primaryKey = buildPrimaryKey(tableName, sinkRecord);

            LinkedHashMap<String, ColumnValue> columnValueMap = buildColumns(tableName, sinkRecord, primaryKey);

            rowChange = buildRowChange(tableName, primaryKey, columnValueMap);
        } catch (TransformException | EventParsingException e) {
            LOGGER.error(String.format("error while transform, table: %s, sinkRecord: %s", tableName, sinkRecord), e);
            throw e;
        } catch (RuntimeException e) {
            LOGGER.error(String.format("error while transform, catch rumtime exception, table: %s, sinkRecord: %s", tableName, sinkRecord), e);
            throw new EventParsingException(e.getMessage());
        }
        return rowChange;
    }


    /**
     * 构建主键
     *
     * @param tableName
     * @param sinkRecord
     * @return PrimaryKey
     */
    private PrimaryKey buildPrimaryKey(String tableName, SinkRecord sinkRecord) throws EventParsingException, TransformException {
        switch (primaryKeyMode) {
            case KAFKA:
                return buildPrimaryKeyByKafka(sinkRecord);
            case RECORD_KEY:
                return buildPrimaryKeyByRecordKey(tableName, sinkRecord);
            case RECORD_VALUE:
                return buildPrimaryKeyByRecordValue(tableName, sinkRecord);
            case SEARCH:
                return buildPrimaryBySearch(sinkRecord);
            default:
                throw new TransformException("Failed to build PrimaryKey");
        }
    }


    private PrimaryKey buildPrimaryBySearch(SinkRecord sinkRecord) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        String source = String.format("%s_%s_%s", sinkRecord.topic(), sinkRecord.kafkaPartition(), sinkRecord.kafkaOffset());
        String md5 = DigestUtils.md5Hex(source).substring(0,5);

        primaryKeyBuilder.addPrimaryKeyColumn(TableStoreSinkConfig.SEARCH_KEY_MD5, PrimaryKeyValue.fromString(md5));
        primaryKeyBuilder.addPrimaryKeyColumn(TableStoreSinkConfig.SEARCH_KEY_TOPIC, PrimaryKeyValue.fromString(sinkRecord.topic()));
        primaryKeyBuilder.addPrimaryKeyColumn(TableStoreSinkConfig.SEARCH_KEY_PARTITION, PrimaryKeyValue.fromLong(sinkRecord.kafkaPartition()));
        primaryKeyBuilder.addPrimaryKeyColumn(TableStoreSinkConfig.SEARCH_KEY_OFFSET, PrimaryKeyValue.fromLong(sinkRecord.kafkaOffset()));

        return primaryKeyBuilder.build();
    }

    /**
     * 根据 Kafka 的 topic，partition和 offset 创建主键
     *
     * @param sinkRecord
     * @return PrimaryKey
     */
    private PrimaryKey buildPrimaryKeyByKafka(SinkRecord sinkRecord) {
        PrimaryKeyBuilder primaryKeyBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        primaryKeyBuilder.addPrimaryKeyColumn(
                TableStoreSinkConfig.PRIMARY_KEY_NAME_TOPIC_PARTITION,
                PrimaryKeyValue.fromString(String.format("%s_%s", sinkRecord.topic(), sinkRecord.kafkaPartition()))
        );

        primaryKeyBuilder.addPrimaryKeyColumn(
                TableStoreSinkConfig.PRIMARY_KEY_NAME_OFFSET,
                PrimaryKeyValue.fromLong(sinkRecord.kafkaOffset()));

        return primaryKeyBuilder.build();
    }

    /**
     * 根据 RecordKey 创建主键
     *
     * @param tableName
     * @param sinkRecord
     * @return PrimaryKey
     */
    private PrimaryKey buildPrimaryKeyByRecordKey(String tableName, SinkRecord sinkRecord) throws EventParsingException, TransformException {
        List<PrimaryKeySchema> pkSchemaList = config.getPrimaryKeySchemaListByTable(tableName);

        Schema schema = sinkRecord.keySchema();
        Object value = sinkRecord.key();

        if (value == null) {
            throw new TransformException("The primary key mode is " + PrimaryKeyMode.RECORD_KEY + ", but the value of record key is null");
        }

        return parser.parseForPrimaryKey(schema, value, pkSchemaList);
    }

    /**
     * 根据 RecordValue 创建主键
     *
     * @param tableName
     * @param sinkRecord
     * @return PrimaryKey
     */
    private PrimaryKey buildPrimaryKeyByRecordValue(String tableName, SinkRecord sinkRecord) throws EventParsingException, TransformException {
        List<PrimaryKeySchema> pkSchemaList = config.getPrimaryKeySchemaListByTable(tableName);

        Schema schema = sinkRecord.valueSchema();
        Object value = sinkRecord.value();

        if (value == null) {
            throw new TransformException("The primary key mode is " + PrimaryKeyMode.RECORD_VALUE + ", but the value of record key is null");
        }

        return parser.parseForPrimaryKey(schema, value, pkSchemaList);
    }

    /**
     * 构建属性列
     *
     * @param tableName
     * @param sinkRecord
     * @return Map<String, ColumnValue>,key为列名，value为列值
     */
    private LinkedHashMap<String, ColumnValue> buildColumns(String tableName, SinkRecord sinkRecord, PrimaryKey primaryKey) throws EventParsingException {
        List<DefinedColumnSchema> whitelistColumnSchemaList = config.getColumnSchemaListByTable(tableName);

        Schema schema = sinkRecord.valueSchema();
        Object value = sinkRecord.value();
        Schema keySchema = sinkRecord.keySchema();
        Object key = sinkRecord.key();

        LinkedHashMap<String, ColumnValue> columnValueMap = parser.parseForColumns(
                keySchema, key, schema, value, primaryKey, whitelistColumnSchemaList, primaryKeyMode, sinkRecord, config, tableName
        );

        if (columnValueMap == null) {
            isDeleteRow = true;
        }

        return columnValueMap;
    }

    /**
     * 构建 RowChange
     *
     * @param tableName
     * @param primaryKey
     * @param columnValueMap
     * @return RowChange
     */
    private RowChange buildRowChange(String tableName, PrimaryKey primaryKey, Map<String, ColumnValue> columnValueMap) throws TransformException {
        //先判断能不能删行
        if (isDeleteRow) {
            //1. SinkRecord 的 Value 为空，判断是否要删行
            if (DeleteMode.ROW.equals(deleteMode) || DeleteMode.ROW_AND_COLUMN.equals(deleteMode)) {
                return new RowDeleteChange(tableName, primaryKey);
            } else {
                if (InsertMode.PUT.equals(insertMode)) {
                    return new RowPutChange(tableName, primaryKey);
                } else {
                    throw new TransformException("The insert mode is " + insertMode + ", but the value of SinkRecord is null.");
                }
            }
        }

        List<Column> columnList = new ArrayList<>();
        List<String> deleteColumnList = new ArrayList<>();
        for (Map.Entry<String, ColumnValue> entry : columnValueMap.entrySet()) {
            if (entry.getValue() != null) {
                columnList.add(new Column(entry.getKey(), entry.getValue()));
            } else {
                deleteColumnList.add(entry.getKey());
            }
        }

        if (InsertMode.PUT.equals(insertMode)) {
            // put 模式:覆盖写
            return new RowPutChange(tableName, primaryKey).addColumns(columnList);
        } else {
            // update 模式:先判断是不是全部为null, 再判断能不能删列
            RowUpdateChange rowUpdateChange = new RowUpdateChange(tableName, primaryKey);
            if (columnList.isEmpty()) {
                //2.1 全部为 null,允许删列就删列，不允许就报错
                if (DeleteMode.COLUMN.equals(deleteMode) || DeleteMode.ROW_AND_COLUMN.equals(deleteMode)) {
                    //允许删列
                    for (String deleteColumnName : deleteColumnList) {
                        rowUpdateChange.deleteColumns(deleteColumnName);
                    }
                } else {
                    throw new TransformException("The insert mode is " + insertMode + ", but the attribute column is Empty.");
                }
            } else {
                //2.2 部分为 null,允许删列就删列，不允许就忽略 null
                if (DeleteMode.COLUMN.equals(deleteMode) || DeleteMode.ROW_AND_COLUMN.equals(deleteMode)) {
                    //允许删列
                    for (String deleteColumnName : deleteColumnList) {
                        rowUpdateChange.deleteColumns(deleteColumnName);
                    }
                }
                rowUpdateChange.put(columnList);
            }

            return rowUpdateChange;

        }
    }

    public InsertMode getInsertMode() {
        return insertMode;
    }

    public void setInsertMode(InsertMode insertMode) {
        this.insertMode = insertMode;
    }
}



