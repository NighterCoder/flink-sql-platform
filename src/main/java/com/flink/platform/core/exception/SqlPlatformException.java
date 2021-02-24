package com.flink.platform.core.exception;

public class SqlPlatformException extends RuntimeException{

    public SqlPlatformException(String msg){
        super(msg);
    }

    public SqlPlatformException(String msg, Throwable cause) {
        super(msg, cause);
    }

}
