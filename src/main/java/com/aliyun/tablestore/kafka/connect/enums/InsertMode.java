package com.aliyun.tablestore.kafka.connect.enums;

public enum InsertMode {
    /**
     * 使用Tablestore 的 PutRow 操作，即新写入一行数据会覆盖原数据
     */
    PUT,
    /**
     * 使用 Tablestore 的 UpdateRow 操作，即更新一行数据，可以增加一行中的属性列，或者更新已存在的属性列的值
     */
    UPDATE
}