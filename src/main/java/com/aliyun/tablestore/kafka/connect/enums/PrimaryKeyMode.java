package com.aliyun.tablestore.kafka.connect.enums;

public enum PrimaryKeyMode {
    /**
     * 以<connect_topic>_<connect_partition>(Kafka 主题和分区，用"_"分隔）
     * 和 <connect_offset>（该记录在分区中的偏移量） 作为 OTS 表的主键
     */
    KAFKA,
    /**
     * 以 SinkRecord.key 中的字段作为 OTS 表的主键
     */
    RECORD_KEY,
    /**
     * 以 SinkRecord.value 中的字段作为 OTS 表的主键
     */
    RECORD_VALUE;
}
