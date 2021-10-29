package com.aliyun.tablestore.kafka.connect.enums;

public enum DeleteMode {
    /**
     * 不允许进行任何删除
     */
    NONE,
    /**
     * 允许删除行
     */
    ROW,
    /**
     * 允许删除属性列
     */
    COLUMN,
    /**
     * 允许删除行和属性列
     */
    ROW_AND_COLUMN
}
