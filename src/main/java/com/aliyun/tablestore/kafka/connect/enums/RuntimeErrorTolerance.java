package com.aliyun.tablestore.kafka.connect.enums;

public enum RuntimeErrorTolerance {
    /**
     * 任何解析错误都将导致 task 立即失败
     */
    NONE,
    /**
     * 跳过产生错误的 SinkRecord，并记录该 SinkRecord
     */
    ALL

}
