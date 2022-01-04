package com.aliyun.tablestore.kafka.connect.writer;

import com.aliyun.tablestore.kafka.connect.model.ErrantSinkRecord;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * @Author lihn
 * @Date 2021/11/26 11:44
 */
public interface TableStoreSinkWriterInterface {
    void initWriter(Set<String> topics);

    boolean needRebuild();

    void rebuildClient();

    List<ErrantSinkRecord> write(Collection<SinkRecord> records);

    void closeWriters();

    void close();
}
