package com.flink.platform.web.common;

import com.flink.platform.web.common.util.Pages;
import lombok.Data;

/**
 * Controller返回结果封装实体类
 */
@Data
public class Result<T> {

    public static final String SUCCESS = "200";

    private String code = SUCCESS;

    private boolean success;

    private String message;

    private T data;

    private Pages page;

    public Result() {
    }

    public static <T> Result newInstance(String code, String message, T data, Pages page) {
        Result result = new Result();
        result.code = code;
        result.success = (code.equalsIgnoreCase(SUCCESS));
        result.message = message;
        result.data = data;
        result.page = page;
        return result;
    }

    public static <T> Result newInstance(String code, String message, T data) {
        return newInstance(code, message, data, null);
    }

    public static <T> Result success() {
        return newInstance(SUCCESS, "", null);
    }

    public static <T> Result success(T data) {
        return newInstance(SUCCESS, "", data);
    }

    public static <T> Result error(String code, String message) {
        return newInstance(code, message, null, null);
    }

    public static <T> Result error(String message) {
        return newInstance("500", message, null, null);
    }




}
