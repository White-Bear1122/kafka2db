package com.wbmd.kafka2db.parser;

public class MessageParseException extends Exception {
    public MessageParseException(String message) {
        super(message);
    }

    public MessageParseException(String message, Throwable cause) {
        super(message, cause);
    }
}

