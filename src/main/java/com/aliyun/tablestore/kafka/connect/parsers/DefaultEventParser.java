package com.aliyun.tablestore.kafka.connect.parsers;

import com.alicloud.openservices.tablestore.core.utils.Bytes;
import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.enums.PrimaryKeyMode;
import com.aliyun.tablestore.kafka.connect.enums.SearchTimeMode;
import com.aliyun.tablestore.kafka.connect.utils.ColumnCoverterUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.*;

/**
 * DefaultEventParser 可以解析 Schema.Type 为 Struct, null, 和 Map的记录
 */
public class DefaultEventParser implements EventParser {

    public DefaultEventParser() {
    }

    @Override
    public String className() {
        return this.getClass().getSimpleName();
    }

    @Override
    public PrimaryKey parseForPrimaryKey(
            Schema schema,
            Object value,
            List<PrimaryKeySchema> pkDefinedInConfig
    ) {

        if (schema == null || (schema != null && Schema.Type.MAP.equals(schema.type()))) {
            return parseMapForPrimaryKey(value, pkDefinedInConfig);
        } else if (Schema.Type.STRUCT.equals(schema.type())) {
            return parseStructForPrimaryKey(schema, value, pkDefinedInConfig);
        } else if (Schema.Type.STRING.equals(schema.type())) {
            return parseStringForPrimaryKey(schema, value, pkDefinedInConfig);
        }
        throw new EventParsingException(String.format("Schema of type %s cannot be supported", schema.type()));

    }

    private PrimaryKey parseStringForPrimaryKey(Schema schema, Object value, List<PrimaryKeySchema> pkDefinedInConfig) {
        PrimaryKeyBuilder primaryKeyBuilder=PrimaryKeyBuilder.createPrimaryKeyBuilder();
        String strValue = (String) value;
        if (1 != pkDefinedInConfig.size()) {
            throw new EventParsingException("number of pk must be 1 in string type");
        }

        for (int i = 0; i < pkDefinedInConfig.size(); i++) {
            PrimaryKeySchema pkSchema = pkDefinedInConfig.get(i);
            String pkName = pkSchema.getName();
            PrimaryKeyType pkType = pkSchema.getType();

            primaryKeyBuilder.addPrimaryKeyColumn(pkName, getPrimaryKeyValue(strValue, Schema.STRING_SCHEMA, pkType));
        }
        return primaryKeyBuilder.build();


    }

    /**
     * 解析 Schema.Type 为 null 和 Map 的 Value 成主键
     *
     * @param value
     * @param pkDefinedInConfig
     */
    private PrimaryKey parseMapForPrimaryKey(
            Object value,
            List<PrimaryKeySchema> pkDefinedInConfig
    ) throws EventParsingException {
        PrimaryKeyBuilder primaryKeyBuilder=PrimaryKeyBuilder.createPrimaryKeyBuilder();
        if(!(value instanceof Map)){
            throw new EventParsingException(String.format("Failed to parse value: %s", value.toString()));
        }

        Map<Object, Object> mapValue = (Map<Object, Object>) value;

        for (PrimaryKeySchema pkSchema : pkDefinedInConfig) {
            String pkName = pkSchema.getName();
            if (!mapValue.containsKey(pkName)) {
                //如果配置为自增列，自动补上占位符
                if (PrimaryKeyOption.AUTO_INCREMENT.equals(pkSchema.getOption())) {
                    primaryKeyBuilder.addPrimaryKeyColumn(pkName,PrimaryKeyValue.AUTO_INCREMENT);
                    continue;
                } else {
                    throw new EventParsingException(String.format("PrimaryKey %s cannot be found.", pkName));
                }
            }

//            //如果用户配置的不是bytes，报错
//            if (PrimaryKeyType.BINARY != pkSchema.getType()) {
//                throw new EventParsingException(
//                        "The type of primary key column " + pkName + " is " + pkSchema.getType() +
//                                " defined in config, but this is expected to be binary."
//                );
//            }

            Object originalValue = mapValue.get(pkName);
            primaryKeyBuilder.addPrimaryKeyColumn(pkName, getPrimaryKeyValue(originalValue.toString(), Schema.STRING_SCHEMA, pkSchema.getType()));
        }
        return primaryKeyBuilder.build();
    }


    /**
     * 解析 Schema.Type 为 Struct 的 Value 成主键
     *
     * @param schema
     * @param value
     * @param pkDefinedInConfig
     */
    private PrimaryKey parseStructForPrimaryKey(
            Schema schema,
            Object value,
            List<PrimaryKeySchema> pkDefinedInConfig
    ) throws EventParsingException {
        PrimaryKeyBuilder primaryKeyBuilder=PrimaryKeyBuilder.createPrimaryKeyBuilder();

        Struct structValue = (Struct) value;

        for (PrimaryKeySchema pkSchema : pkDefinedInConfig) {
            String pkName = pkSchema.getName();
            Field field = schema.field(pkName);

            if (field == null) {
                //如果配置为自增列，自动补上占位符
                if (PrimaryKeyOption.AUTO_INCREMENT.equals(pkSchema.getOption())) {
                    primaryKeyBuilder.addPrimaryKeyColumn(pkName, PrimaryKeyValue.AUTO_INCREMENT);
                    continue;
                } else {
                    throw new EventParsingException(String.format("PrimaryKey %s cannot be found.", pkName));
                }
            }

            PrimaryKeyType pkType = convertToPrimaryKeyType(field.schema().type());

            //如果转换后的类型与用户配置不同，报错
            if (pkType != pkSchema.getType()) {
                throw new EventParsingException(
                        "The type of primary key column " + pkName + " is " + pkSchema.getType() +
                                " defined in config, but it's " + pkType + " in SinkRecord."
                );
            }

            primaryKeyBuilder.addPrimaryKeyColumn(pkName, getPrimaryKeyValue(structValue.get(field), field.schema(), pkType));
        }
        return primaryKeyBuilder.build();
    }

    @Override
    public LinkedHashMap<String, ColumnValue> parseForColumns(
            Schema keySchema,
            Object keyValue,
            Schema schema,
            Object value,
            PrimaryKey primaryKey,
            List<DefinedColumnSchema> whitelistColumnSchemaList,
            PrimaryKeyMode primaryKeyMode,
            SinkRecord sinkRecord,
            TableStoreSinkConfig config,
            String tableName
    ) throws EventParsingException {

        LinkedHashMap<String, ColumnValue> result = new LinkedHashMap<>();

        if (value != null) {
            // value 处理
            if (schema == null || (schema != null && Schema.Type.MAP.equals(schema.type()))) {
                result.putAll(parseMapForColumns(value, primaryKey, whitelistColumnSchemaList));
            } else if (Schema.Type.STRUCT.equals(schema.type())) {
                result.putAll(parseStructForColumns(schema, value, primaryKey, whitelistColumnSchemaList));
            } else if (Schema.Type.STRING.equals(schema.type())) {
                if (primaryKeyMode != PrimaryKeyMode.RECORD_VALUE) {
                    result.putAll(parseStringForColumns(value, "value"));
                }
            }
        }

        if (keyValue != null) {
            // key 处理
            if (keySchema == null || (keySchema != null && Schema.Type.MAP.equals(keySchema.type()))) {
                result.putAll(parseMapForColumns(keyValue, primaryKey, whitelistColumnSchemaList));
            } else if (Schema.Type.STRUCT.equals(keySchema.type())) {
                result.putAll(parseStructForColumns(keySchema, keyValue, primaryKey, whitelistColumnSchemaList));
            } else if (Schema.Type.STRING.equals(keySchema.type())) {
                if (primaryKeyMode != PrimaryKeyMode.RECORD_KEY) {
                    result.putAll(parseStringForColumns(keyValue,"key"));
                }
            }
        }

        if (primaryKeyMode == PrimaryKeyMode.SEARCH) {
            if (config.getSearchTimeMode(tableName) == SearchTimeMode.KAFKA) {
                if (sinkRecord.timestamp() == null) {
                    throw new EventParsingException(String.format("timestamp in sink record should not be null, record: %s", sinkRecord));
                }
                result.put(TableStoreSinkConfig.SEARCH_FIELD_TIMESTAMP, ColumnValue.fromLong(sinkRecord.timestamp()));
            } else {
                result.put(TableStoreSinkConfig.SEARCH_FIELD_TIMESTAMP, ColumnValue.fromLong(System.currentTimeMillis()));
            }
        }

        if (result.isEmpty()) {
            throw new EventParsingException(String.format("sink record parse fail, record: %s", sinkRecord));
        }

        return result;
    }


    @Override
    public LinkedHashMap<String, ColumnValue> parseForColumns(
            Schema schema,
            Object value,
            PrimaryKey primaryKey,
            List<DefinedColumnSchema> whitelistColumnSchemaList

    ) throws EventParsingException {
        return parseForColumns(null, null, schema, value, primaryKey, whitelistColumnSchemaList, null, null, null, null);
    }

    /**
     * 当使用stringconverter时，输入数据类型为string，组装column值
     * @param value
     * @return
     */
    private LinkedHashMap<String, ColumnValue> parseStringForColumns(Object value, String key) {
        LinkedHashMap<String, ColumnValue> map = new LinkedHashMap<>();
        map.put(key, ColumnValue.fromString(value.toString()));
        return map;
    }

    /**
     * 解析 Schema.Type 为 null 和 Map 的 Value 成属性列
     *
     * @param value
     * @param primaryKey
     * @param whitelistColumnSchemaList
     */
    private LinkedHashMap<String, ColumnValue> parseMapForColumns(Object value, PrimaryKey primaryKey, List<DefinedColumnSchema> whitelistColumnSchemaList) throws EventParsingException {
        LinkedHashMap<String, ColumnValue> columnValueMap = new LinkedHashMap<>();
        if (!(value instanceof Map)) {
            throw new EventParsingException(String.format("Failed to parse value: %s", value.toString()));
        }
        HashMap<Object, Object> mapValue = (HashMap<Object, Object>) value;

        if (whitelistColumnSchemaList.isEmpty()) {
            //没有过滤列，全写入情况
            for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
                if (!(entry.getKey() instanceof String)) {
                    throw new EventParsingException(String.format("Failed to parse value: %s", value.toString()));
                }
                String colName = (String) entry.getKey();
                if (primaryKey.contains(colName)) {
                    continue;
                }

                columnValueMap.put(colName, ColumnValue.fromString(entry.getValue().toString()));
            }
        } else {
            //有过滤列
            for (DefinedColumnSchema whitelistColumnSchema : whitelistColumnSchemaList) {
                String colName = whitelistColumnSchema.getName();
                if (!mapValue.containsKey(colName)) {
                    continue;
                }

//                //如果用户配置的不是bytes，报错
//                if (DefinedColumnType.BINARY != whitelistColumnSchema.getType()) {
//                    throw new EventParsingException(
//                            "The type of attribute column " + colName + " is " + whitelistColumnSchema.getType() +
//                                    " defined in config, but this is expected to be binary."
//                    );
//                }

                if (primaryKey.contains(colName)) {
                    continue;
                }

                Object originalValue = mapValue.get(colName);

                ColumnValue colValue = ColumnCoverterUtil.getColumnValue(originalValue, null, whitelistColumnSchema.getType());
                columnValueMap.put(colName, colValue);
            }

        }
        return columnValueMap;
    }

    /**
     * 解析 Schema.Type 为 null 和 Map 的 Value 成属性列
     *
     * @param schema
     * @param value
     * @param primaryKey
     * @param whitelistColumnSchemaList
     */
    private LinkedHashMap<String, ColumnValue> parseStructForColumns(Schema schema, Object value, PrimaryKey primaryKey, List<DefinedColumnSchema> whitelistColumnSchemaList) throws EventParsingException {
        LinkedHashMap<String, ColumnValue> columnValueMap = new LinkedHashMap<>();

        Struct structValue = (Struct) value;

        if (whitelistColumnSchemaList.isEmpty()) {
            //没有过滤列，全写入情况
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                if (primaryKey.contains(field.name())) {
                    continue;
                }
                DefinedColumnType colType = ColumnCoverterUtil.convertToColumnType(field.schema().type());
                columnValueMap.put(field.name(), ColumnCoverterUtil.getColumnValue(structValue.get(field), field.schema(), colType));
            }
        } else {
            //有过滤列
            for (DefinedColumnSchema whitelistColumnSchema : whitelistColumnSchemaList) {
                String colName = whitelistColumnSchema.getName();
                Field field = schema.field(colName);
                if (field == null) {
                    continue;
                }
                if (primaryKey.contains(field.name())) {
                    continue;
                }

                DefinedColumnType colType = ColumnCoverterUtil.convertToColumnType(field.schema().type());

                //如果转换后的类型与用户配置不同，报错
                if (colType != whitelistColumnSchema.getType()) {
                    throw new EventParsingException(
                            "The type of attribute column '" + colName + " is " + whitelistColumnSchema.getType() +
                                    "defined in config, but it's " + colType + " in SinkRecord."
                    );
                }
                ColumnValue colValue = ColumnCoverterUtil.getColumnValue(structValue.get(field), field.schema(), colType);
                columnValueMap.put(colName, colValue);
            }
        }

        return columnValueMap;
    }

    /**
     * 转换为bytes
     *
     * @param originalValue
     */
    private byte[] convertToBytes(Object originalValue) {
        if (originalValue == null) {
            return null;
        }
        Class<?> clazz = originalValue.getClass();
        String type = clazz.getSimpleName().toUpperCase();
        switch (type) {
            case "STRING":
                return Bytes.toBytes((String) originalValue);
            case "BYTE":
                return new byte[]{(byte) originalValue};
            case "SHORT":
                return Bytes.toBytes((Short)originalValue);
            case "INTEGER":
                return Bytes.toBytes((Integer) originalValue);
            case "LONG":
                return Bytes.toBytes((Long) originalValue);
            case "FLOAT":
                return Bytes.toBytes((Float) originalValue);
            case "DOUBLE":
                return Bytes.toBytes((Double) originalValue);
            case "BOOLEAN":
                return Bytes.toBytes((Boolean) originalValue);
            case "BYTE[]":
                return (byte[]) originalValue;
            default:
                throw new EventParsingException(String.format("Unexpected type: %s", type));
        }
    }

    /**
     * 主键列数据类型映射
     *
     * @param type
     * @return PrimaryKeyType
     */
    public PrimaryKeyType convertToPrimaryKeyType(Schema.Type type) {
        switch (type) {
            case STRING:
                return PrimaryKeyType.STRING;
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return PrimaryKeyType.INTEGER;
            case BYTES:
                return PrimaryKeyType.BINARY;
            default:
                throw new EventParsingException(String.format("Unexpected type for PrimaryKey: %s", type));
        }

    }

    /**
     * 转换成主键值
     *
     * @param value
     * @param pkType
     * @return PrimaryKeyValue
     */
    private PrimaryKeyValue getPrimaryKeyValue(Object value, Schema schema, PrimaryKeyType pkType) {
        if (value == null) {
            return null;
        }
        switch (pkType) {
            case STRING:
                return PrimaryKeyValue.fromString(Values.convertToString(schema, value));
            case INTEGER:
                return PrimaryKeyValue.fromLong(Values.convertToLong(schema, value));
            case BINARY:
                return PrimaryKeyValue.fromBinary((byte[]) (value));
            default:
                throw new EventParsingException(String.format("Failed to convert %s to PrimaryKeyValue", value.toString()));
        }

    }


}
