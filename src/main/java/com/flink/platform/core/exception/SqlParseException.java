package com.flink.platform.core.exception;

public class SqlParseException extends RuntimeException {

    private static final long serialVersionUID = 1L;

	public SqlParseException(String message) {
        super(message);
    }

	public SqlParseException(String message, Throwable e) {
        super(message, e);
    }
}
