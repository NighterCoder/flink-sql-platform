package com.flink.platform.web.exception;

/**
 * Created by 凌战 on 2021/3/3
 */
public class StreamNodeParseException extends RuntimeException {

    public StreamNodeParseException(String msg) {
        super(msg);
    }

    public StreamNodeParseException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
