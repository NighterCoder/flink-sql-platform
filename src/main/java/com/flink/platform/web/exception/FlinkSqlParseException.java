package com.flink.platform.web.exception;

/**
 * Created by 凌战 on 2021/3/23
 */
public class FlinkSqlParseException extends RuntimeException {

    public FlinkSqlParseException(String msg) {
        super(msg);
    }

    public FlinkSqlParseException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
