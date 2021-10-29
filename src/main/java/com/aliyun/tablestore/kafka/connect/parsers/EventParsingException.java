package com.aliyun.tablestore.kafka.connect.parsers;

public class EventParsingException extends RuntimeException {

    public EventParsingException() {
        super();
    }

    public EventParsingException(String message) {
        super(message);
    }

    public EventParsingException(String message, Throwable cause) {
        super(message, cause);
    }

    public EventParsingException(Throwable cause) {
        super(cause);
    }

    protected EventParsingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
