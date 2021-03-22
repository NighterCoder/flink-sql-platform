package com.flink.platform.web.common.util;

import com.alibaba.fastjson.JSONObject;
import com.flink.platform.core.exception.SqlPlatformException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.*;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

/**
 * HTTP REST API调用工具类
 */
@Slf4j
public class HttpUtils {

    public static JSONObject post(String url,Object body){

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        /**
         * 构造函数:
         * 1. 请求参数,一般是HashMap
         * 2. Header参数
         */
        HttpEntity<Object> request = new HttpEntity<>(body,headers);


        RestTemplate restTemplate = new RestTemplate();
        restTemplate.setErrorHandler(new CustomResponseErrorHandler());
        /**
         * 构造函数:
         * 1. 请求url
         * 2. 请求参数request
         * 3. 返回结果类型
         */
        ResponseEntity<String> json = restTemplate.postForEntity(url, request, String.class);
        int code = json.getStatusCodeValue();
        if (code == HttpStatus.OK.value() || code == HttpStatus.CREATED.value()) {
            return JSONObject.parseObject(json.getBody());
        } else {
            log.error("错误码：{}, 错误描述：{}", json.getStatusCodeValue(), json.getBody());
            throw new SqlPlatformException(json.getBody());
        }
    }



    static class CustomResponseErrorHandler implements ResponseErrorHandler{

        @Override
        public boolean hasError(ClientHttpResponse clientHttpResponse) throws IOException {
            return false;
        }

        @Override
        public void handleError(ClientHttpResponse clientHttpResponse) throws IOException {

        }
    }



}
