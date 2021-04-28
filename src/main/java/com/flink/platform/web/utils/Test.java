package com.flink.platform.web.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by 凌战 on 2021/4/26
 */
public class Test {

    private static final Map<String, String> HEADERS;

    static {
        HEADERS = new HashMap<>();
        // 实体头,表示发送端(客户端/服务器发送的实体数据类型)
        HEADERS.put("Content-Type", "application/json");
        // 请求头,表示发送端(客户端)希望接受的数据类型
        HEADERS.put("Accept", "application/json; charset=UTF-8");
    }


    public static void main(String[] args) {

        YarnApiUtils.backpressure("http://phm-data01:8088","application_1619077805256_0031");


    }

}
