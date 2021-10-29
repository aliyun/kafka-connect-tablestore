

package com.aliyun.tablestore.kafka.connect.utils;

import com.alicloud.openservices.tablestore.model.DefinedColumnSchema;
import com.alicloud.openservices.tablestore.model.PrimaryKeySchema;
import com.alicloud.openservices.tablestore.model.TableMeta;
import org.apache.kafka.connect.errors.ConnectException;

import java.util.List;
import java.util.Map;

public class ParamChecker {
    /**
     * 检查用户配置是否与实际表结构相同
     * 1. 检查主键列名和数据类型
     * 2.检查属性列是否与主键列冲突，是否超出最大限制，是否与预定义属性列冲突
     *
     * @param tableMeta
     * @param pkDefinedInConfig
     * @param colDefinedInConfig
     * @param maxColumnsCount
     */
    public static void checkTable(TableMeta tableMeta, List<PrimaryKeySchema> pkDefinedInConfig, List<DefinedColumnSchema> colDefinedInConfig, int maxColumnsCount) {
        //检查主键
        List<PrimaryKeySchema> pkDefinedInMeta = tableMeta.getPrimaryKeyList();
        checkPrimaryKey(pkDefinedInMeta, pkDefinedInConfig);

        //检查属性列
        Map<String, PrimaryKeySchema> pkDefinedInMetaMap = tableMeta.getPrimaryKeySchemaMap();
        Map<String, DefinedColumnSchema> definedColumnSchemaMap = tableMeta.getDefinedColumnSchemaMap();
        ParamChecker.checkColumn(pkDefinedInMetaMap, definedColumnSchemaMap, colDefinedInConfig, maxColumnsCount);
    }

    /**
     * 检查主键列名和数据类型
     *
     * @param pkDefinedInMeta
     * @param pkDefinedInConfig
     */
    public static void checkPrimaryKey(List<PrimaryKeySchema> pkDefinedInMeta, List<PrimaryKeySchema> pkDefinedInConfig) {
        if (pkDefinedInMeta.size() != pkDefinedInConfig.size()) {
            throw new ConnectException("The primary key schema is not match which defined in config.");
        }

        for (int i = 0; i < pkDefinedInConfig.size(); ++i) {
            PrimaryKeySchema pkSchemaInMeta = pkDefinedInMeta.get(i);
            PrimaryKeySchema pkSchemaInConfig = pkDefinedInConfig.get(i);

            if (!pkSchemaInMeta.getName().equals(pkSchemaInConfig.getName())) {
                throw new ConnectException("The name of primary key column is " + pkSchemaInMeta.getName() +
                        ", but it's defined as " + pkSchemaInConfig.getName() + " in config.");
            }

            if (pkSchemaInMeta.getType() != pkSchemaInConfig.getType()) {
                throw new ConnectException("The type of primary key column '" + pkSchemaInMeta.getName() + " is " + pkSchemaInMeta.getType() +
                        ", but it's defined as " + pkSchemaInConfig.getType() + " in config.");
            }

            if (pkSchemaInConfig.getOption() != pkSchemaInMeta.getOption()) {
                throw new ConnectException("The type of primary key column '" + pkSchemaInConfig.getName() + "' should not be AUTO_INCREMENT.");
            }

        }
    }

    /**
     * 检查属性列是否超出最大限制,是否与主键列冲突，是否与预定义属性列冲突
     *
     * @param pkDefinedInMeta
     * @param definedColumnSchemaMap
     * @param colDefinedInConfig
     * @param maxColumnsCount
     */
    public static void checkColumn(Map<String, PrimaryKeySchema> pkDefinedInMeta, Map<String, DefinedColumnSchema> definedColumnSchemaMap, List<DefinedColumnSchema> colDefinedInConfig, int maxColumnsCount) {
        //检查属性列是否超出最大限制
        int columnsCount = colDefinedInConfig.size();
        if (columnsCount > maxColumnsCount) {
            throw new ConnectException("The count of attribute columns exceeds the maximum: " + maxColumnsCount + ".");
        }

        //检查属性列是否和主键重复,是否与预定义属性列冲突
        for (DefinedColumnSchema columnSchema : colDefinedInConfig) {
            if (pkDefinedInMeta.containsKey(columnSchema.getName())) {
                throw new ConnectException("The attribute column's name duplicate with primary key column, which is '" + columnSchema.getName() + "'.");
            }

            if (definedColumnSchemaMap.containsKey(columnSchema.getName())) {
                if (!definedColumnSchemaMap.get(columnSchema.getName()).getType().equals(columnSchema.getType())) {
                    throw new ConnectException("The type of attribute column '" + columnSchema.getName() + " is " + definedColumnSchemaMap.get(columnSchema.getName()).getType() +
                            ", but it's defined as " + columnSchema.getType() + " in config.");
                }
            }

        }

    }
}
