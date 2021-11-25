package com.aliyun.tablestore.kafka.connect.parsers;

import com.alicloud.openservices.tablestore.model.*;
import com.aliyun.tablestore.kafka.connect.enums.PrimaryKeyMode;
import org.apache.kafka.connect.data.Schema;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface EventParser {

    /**
     * 返回解析器名称
     *
     * @return the parser's name
     */
    String className();


    /**
     * 解析成主键
     *
     * @param schema
     * @param value
     * @param pkDefinedInConfig
     * @return Map<String, PrimaryKeyValue>
     */
    PrimaryKey parseForPrimaryKey(
            Schema schema,
            Object value,
            List<PrimaryKeySchema> pkDefinedInConfig
    ) throws EventParsingException;

    /**
     * 解析成属性列
     *
     * @param schema
     * @param value
     * @param primaryKey
     * @param whitelistColumnSchemaList
     * @return Map<String, ColumnValue> 如果返回 null，则表示删行
     */
    LinkedHashMap<String, ColumnValue> parseForColumns(
            Schema keySchema,
            Object key,
            Schema schema,
            Object value,
            PrimaryKey primaryKey,
            List<DefinedColumnSchema> whitelistColumnSchemaList,
            PrimaryKeyMode primaryKeyMode
    ) throws EventParsingException;

    LinkedHashMap<String, ColumnValue> parseForColumns(
            Schema schema,
            Object value,
            PrimaryKey primaryKey,
            List<DefinedColumnSchema> whitelistColumnSchemaList
    ) throws EventParsingException;

}
