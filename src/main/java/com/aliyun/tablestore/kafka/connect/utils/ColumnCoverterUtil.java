package com.aliyun.tablestore.kafka.connect.utils;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.DefinedColumnType;
import com.aliyun.tablestore.kafka.connect.parsers.EventParsingException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Values;

/**
 * @Author lihn
 * @Date 2021/12/7 10:43
 */
public class ColumnCoverterUtil {



    /**
     * 转换成属性列值
     *
     * @param value
     * @param colType
     * @return ColumnValue
     */
    public static ColumnValue getColumnValue(Object value, Schema schema, DefinedColumnType colType) {
        if (value == null) {
            return null;
        }
        switch (colType) {
            case STRING:
                return ColumnValue.fromString(Values.convertToString(schema, value));
            case INTEGER:
                return ColumnValue.fromLong(Values.convertToLong(schema, value));
            case BINARY:
                if (value instanceof byte[]) {
                    return ColumnValue.fromBinary((byte[]) (value));
                } else if (value instanceof String) {
                    return ColumnValue.fromBinary(((String) value).getBytes());
                } else {
                    return ColumnValue.fromBinary(value.toString().getBytes());
                }
            case DOUBLE:
                return ColumnValue.fromDouble(Values.convertToDouble(schema, value));
            case BOOLEAN:
                return ColumnValue.fromBoolean(Values.convertToBoolean(schema, value));
            default:
                throw new EventParsingException(String.format("Failed to convert %s to ColumnValue", value.toString()));
        }

    }

    /**
     * 属性列数据类型映射
     *
     * @param type
     * @return DefinedColumnType
     */
    public static DefinedColumnType convertToColumnType(Schema.Type type) {
        switch (type) {
            case STRING:
                return DefinedColumnType.STRING;
            case INT8:
            case INT16:
            case INT32:
            case INT64:
                return DefinedColumnType.INTEGER;
            case BYTES:
                return DefinedColumnType.BINARY;
            case FLOAT32:
            case FLOAT64:
                return DefinedColumnType.DOUBLE;
            case BOOLEAN:
                return DefinedColumnType.BOOLEAN;
            default:
                throw new EventParsingException(String.format("Unexpected type for Column: %s", type));
        }
    }


    public static DefinedColumnType getObjectType(Object ob) {
        if (ob instanceof Number) {
//        if (ob instanceof Integer || ob instanceof Long ||  ob instanceof Short) {
//            return DefinedColumnType.INTEGER;
//        } else if (ob instanceof Number) {
            return DefinedColumnType.DOUBLE;
        } else if (ob instanceof byte[]) {
            return DefinedColumnType.BINARY;
        } else if (ob instanceof Boolean) {
            return DefinedColumnType.BOOLEAN;
        } else {
            return DefinedColumnType.STRING;
        }
    }

}
