package com.aliyun.tablestore.kafka.connect.model;

import org.apache.kafka.connect.sink.SinkRecord;

public class ErrantSinkRecord {
    private SinkRecord sinkRecord;
    private Throwable error;

    public ErrantSinkRecord(SinkRecord record,Throwable error){
        this.sinkRecord=record;
        this.error=error;
    }

    public SinkRecord getSinkRecord() {
        return sinkRecord;
    }

    public Throwable getError() {
        return error;
    }

    public void setSinkRecord(SinkRecord sinkRecord) {
        this.sinkRecord = sinkRecord;
    }
}
