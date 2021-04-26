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

        OkHttpUtils.Result result = OkHttpUtils.doGet("http://phm-data01:8088/proxy/application_1619077805256_0011/jobs/70993486bdccc00ce3e3c0de2e827c28/vertices/bd9c186e89cc37a76d776844dfa8ccba/backpressure",null,HEADERS);
        System.out.println(result.isSuccessful);


    }

}
