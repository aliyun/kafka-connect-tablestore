package com.aliyun.tablestore.kafka.connect.errors;

import org.apache.kafka.common.errors.InvalidTimestampException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;

import java.util.HashMap;
import java.util.Map;

public abstract class GenericErrorReporter implements ErrorReporter {


    protected HeaderConverter headerConverter;
    protected Converter keyConverter;
    protected Converter valueConverter;


    public GenericErrorReporter() {
        headerConverter = createHeaderConverter();
        keyConverter = createKeyConverter();
        valueConverter = createValueConverter();
    }


    /**
     * Kafka 消息 Header序列化转换器，默认使用 JsonConverter
     */
    protected HeaderConverter createHeaderConverter() {
        Map<String, String> converterProps = new HashMap<>();
        converterProps.put(JsonConverterConfig.TYPE_CONFIG, "header");
        converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");

        HeaderConverter converter = new JsonConverter();
        converter.configure(converterProps);
        return converter;
    }

    /**
     * Kafka 消息 Key 序列化转换器，默认使用 JsonConverter
     */
    protected Converter createKeyConverter() {
        Map<String, String> converterProps = new HashMap<>();
        converterProps.put(JsonConverterConfig.TYPE_CONFIG, "key");
        converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");

        Converter converter = new JsonConverter();
        converter.configure(converterProps, true);//isKey=true
        return converter;
    }

    /**
     * Kafka 消息 Value 序列化转换器，默认使用 JsonConverter
     */
    protected Converter createValueConverter() {
        Map<String, String> converterProps = new HashMap<>();
        converterProps.put(JsonConverterConfig.TYPE_CONFIG, "value");
        converterProps.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");

        Converter converter = new JsonConverter();
        converter.configure(converterProps, false);//isKey=false
        return converter;
    }

    /**
     * 将错误转换为字符串
     */
    protected String convertError(Throwable error) {
        String errorType = error.getClass().getSimpleName();
        String message = error.getMessage();
        return "Error: " + errorType + ", Message: " + message;
    }

    /**
     * 对 SinkRecord的时间戳进行检查
     */
    public static Long checkAndConvertTimestamp(Long timestamp) {
        if (timestamp != null && timestamp < 0L) {
            if (timestamp == -1L) {
                return null;
            } else {
                throw new InvalidTimestampException(String.format("Invalid record timestamp %d", timestamp));
            }
        } else {
            return timestamp;
        }
    }
}
