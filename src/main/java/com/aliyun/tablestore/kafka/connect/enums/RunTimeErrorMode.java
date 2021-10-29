package com.aliyun.tablestore.kafka.connect.enums;

public enum RunTimeErrorMode {
    /**
     * 忽略脏数据
     */
    IGNORE,
    /**
     * 将脏数据存储在Kafka中
     */
    KAFKA,
    /**
     * 将脏数据存储在另一张 OTS 表中
     */
    TABLESTORE
}
