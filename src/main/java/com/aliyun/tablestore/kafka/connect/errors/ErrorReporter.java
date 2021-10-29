package com.aliyun.tablestore.kafka.connect.errors;

import com.aliyun.tablestore.kafka.connect.model.ErrantSinkRecord;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.List;

public interface ErrorReporter {
    /**
     * 返回错误报告器名称
     *
     * @return the parser's name
     */
    String className();

    /**
     * 将产生错误的 SinkRecord 复制到指定数据系统
     *
     * @param errantRecords
     */
    default void report(List<ErrantSinkRecord> errantRecords) {
        for (ErrantSinkRecord errantSinkRecord : errantRecords) {
            report(errantSinkRecord.getSinkRecord(), errantSinkRecord.getError());
        }
        flush();
    }

    /**
     * 将产生错误的 SinkRecord 复制到指定数据系统
     *
     * @param errantRecord
     */
    void report(SinkRecord errantRecord, Throwable error);

    /**
     * 刷新缓冲队列
     */
    void flush();

    /**
     * 关闭 Reporter
     */
    void close();
}
