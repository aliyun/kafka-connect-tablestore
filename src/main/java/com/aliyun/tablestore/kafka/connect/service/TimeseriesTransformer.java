package com.aliyun.tablestore.kafka.connect.service;

import com.alicloud.openservices.tablestore.core.utils.StringUtils;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.DefinedColumnSchema;
import com.alicloud.openservices.tablestore.model.DefinedColumnType;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesKey;
import com.alicloud.openservices.tablestore.model.timeseries.TimeseriesRow;
import com.aliyun.tablestore.kafka.connect.TableStoreSinkConfig;
import com.aliyun.tablestore.kafka.connect.parsers.EventParsingException;
import com.aliyun.tablestore.kafka.connect.utils.ColumnCoverterUtil;
import com.aliyun.tablestore.kafka.connect.utils.TransformException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @Author lihn
 * @Date 2021/12/2 20:32
 */
public class TimeseriesTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesTransformer.class);

    private TableStoreSinkConfig config;

    public TimeseriesTransformer(TableStoreSinkConfig config) {
        this.config = config;
    }


    public TimeseriesRow transform(String tableName, SinkRecord sinkRecord) throws EventParsingException, TransformException {
        TimeseriesRow r = null;
        try {
            TimeseriesKey primaryKey = buildPrimaryKey(tableName, sinkRecord);
            long time = parseTime(tableName, sinkRecord);
            // 取时间
            r = new TimeseriesRow(primaryKey, time);

            // 填充field
            LinkedHashMap<String, ColumnValue> columnValueMap = getFields(tableName, sinkRecord, primaryKey);
            for (Map.Entry<String, ColumnValue> entry : columnValueMap.entrySet()) {
                r.addField(entry.getKey(), entry.getValue());
            }

            return r;
        } catch (TransformException | EventParsingException e) {
            LOGGER.error(String.format("error while transform, table: %s, sinkRecord: %s", tableName, sinkRecord), e);
            throw e;
        } catch (RuntimeException e) {
            LOGGER.error(String.format("error while transform, catch rumtime exception, table: %s, sinkRecord: %s", tableName, sinkRecord), e);
            throw new EventParsingException(e.getMessage());
        }
    }

    private long parseTime(String tableName, SinkRecord sinkRecord) {
        checkSinkRecordMap(sinkRecord);
        String timeKey = config.originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_TIME, tableName), "");
        if (timeKey == null || timeKey.isEmpty()) {
            throw new EventParsingException(String.format("key %s do not exist in config.", String.format(TableStoreSinkConfig.TIMESERIES_TIME, tableName)));
        }
        Map<Object, Object> mapValue = (Map<Object, Object>) sinkRecord.value();
        if (!mapValue.containsKey(timeKey)) {
            throw new EventParsingException(String.format("key %s do not exist in record.", String.format(TableStoreSinkConfig.TIMESERIES_TIME, tableName)));
        }

        String unitStr = config.originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_TIME_UNIT, tableName), "");
        TimeUnit unit = null;
        try {
            unit = TimeUnit.valueOf(unitStr.toUpperCase());
        } catch (Exception e) {
            throw new EventParsingException(String.format("no time unit config for %s. Expect SECONDS/MILLISECONDS/MICROSECONDS/NANOSECONDS, find %s.", tableName, unitStr));
        }
        if (unit == null) {
            throw new EventParsingException(String.format("no time unit config for %s. Expect SECONDS/MILLISECONDS/MICROSECONDS/NANOSECONDS, find %s.", tableName, unitStr));
        }

        long timeValue = (long)mapValue.get(timeKey);
        timeValue = unit.toMicros(timeValue);

        return timeValue;
    }


    private LinkedHashMap<String, ColumnValue> getFields(String tableName, SinkRecord sinkRecord, TimeseriesKey primaryKey) {

        checkSinkRecordMap(sinkRecord);

        Object value = sinkRecord.value();

        LinkedHashMap<String, ColumnValue> columnValueMap = parseForColumnsByValue(
                tableName, value, primaryKey
        );

        if (columnValueMap == null || columnValueMap.isEmpty()) {
            throw new EventParsingException("field value is empty, please check config or input data.");
        }

        return columnValueMap;
    }

    private LinkedHashMap<String, ColumnValue> parseForColumnsByValue(String tableName, Object value, TimeseriesKey primaryKey) {
        if (!(value instanceof Map)) {
            throw new EventParsingException("record value should be of type map");
        }

        Map<String, Object> mapValue = (Map<String, Object>) value;

        Set<String> keyStrings = config.getTimeseriesKeyStrings(tableName);

        Map<String, Object> fields = new HashMap<>();
        for (Map.Entry<String, Object> entry : mapValue.entrySet()) {
            if (keyStrings.contains(entry.getKey())) {
                continue;
            }

            fields.put(entry.getKey(), entry.getValue());
        }

        LinkedHashMap<String, ColumnValue> result = new LinkedHashMap<>();

        Map<String, DefinedColumnSchema> schemaMap = config.getTimeseriesColumnSchemaMapByTable(tableName);

        boolean mapAll = config.timeseriesMapAll();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            ColumnValue colValue = null;
            if (schemaMap.containsKey(entry.getKey().toLowerCase())) {
                // 如果已经配置了字段类型
                DefinedColumnType colType = schemaMap.get(entry.getKey().toLowerCase()).getType();
                colValue = ColumnCoverterUtil.getColumnValue(entry.getValue(), null, colType);
            } else if (mapAll) {
                // 如果没有配置字段类型，并且需要map所有字段的话
                Object ob = entry.getValue();
                DefinedColumnType colType = ColumnCoverterUtil.getObjectType(ob);
                colValue = ColumnCoverterUtil.getColumnValue(ob, null, colType);
            }

            if (colValue != null) {
                if (config.toLowerCaseTimeseries()) {
                    result.put(entry.getKey().toLowerCase(), colValue);
                } else {
                    result.put(entry.getKey(), colValue);
                }
            }
        }

        return result;
    }

    private TimeseriesKey buildPrimaryKey(String tableName, SinkRecord sinkRecord) {
        return buildPrimaryKeyByRecordValue(tableName, sinkRecord);

    }


    private TimeseriesKey buildPrimaryKeyByRecordValue(String tableName, SinkRecord sinkRecord) {
        checkSinkRecordMap(sinkRecord);

        Map<Object, Object> mapValue = (Map<Object, Object>) sinkRecord.value();

        String mName = config.originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_MNAME, tableName), "");
        String datasource = config.originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_DATASOURCE, tableName), "");
        String tagValue = config.originalsStrings().getOrDefault(String.format(TableStoreSinkConfig.TIMESERIES_TAGS, tableName), "");

        if (mName == null || mName.isEmpty()) {
            throw new EventParsingException(String.format("key %s do not exist in config.", String.format(TableStoreSinkConfig.TIMESERIES_MNAME, tableName)));
        }

        String measurement = "";
        if (mName.equalsIgnoreCase(TableStoreSinkConfig.TIMESERIES_MNAME_TOPIC)) {
            measurement = sinkRecord.topic();
        } else if (mapValue.containsKey(mName) && mapValue.get(mName) != null) {
            measurement = mapValue.get(mName).toString();
        } else {
            throw new EventParsingException(String.format("key %s do not exist in record.", mName));
        }

        String datasourceValue = null;
        if ((!StringUtils.isNullOrEmpty(datasource)) && !mapValue.containsKey(datasource)) {
            throw new EventParsingException(String.format("key %s do not exist in record.", datasource));
        } else if (!StringUtils.isNullOrEmpty(datasource) && mapValue.containsKey(datasource) && mapValue.get(datasource) != null) {
            datasourceValue = mapValue.get(datasource).toString();
        }

        Map<String, String> tagMap = new HashMap<>();
        if (!StringUtils.isNullOrEmpty(tagValue)) {
            for (String str : tagValue.split(",")) {
                if (mapValue.containsKey(str) && mapValue.get(str) != null) {
                    tagMap.put(str, mapValue.get(str).toString());
                }
            }
        }

        return new TimeseriesKey(measurement, datasourceValue, tagMap);
    }



    public void checkSinkRecordMap(SinkRecord sinkRecord) {
        if (sinkRecord == null || sinkRecord.value() == null) {
            throw new EventParsingException(String.format("sink record value is empty"));
        }
        if (!(sinkRecord.value() instanceof Map)) {
            throw new EventParsingException(String.format("sink record value should be type map"));
        }
    }

}
