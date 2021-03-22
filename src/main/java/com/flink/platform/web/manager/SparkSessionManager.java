package com.flink.platform.web.manager;

import com.alibaba.fastjson.JSONObject;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.spark.SparkSessionDTO;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.util.HttpUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 集成Apache Livy
 * 1. 属性配置
 * 2. 创建Session
 *
 *
 *
 */
@Slf4j
@Component
public class SparkSessionManager implements SessionManager {

    @Value("${spark.livy.url}")
    public String url;

    @Value("${spark.livy.hive-file}")
    public String hiveFile;

    @Value("${spark.dynamicAllocation}")
    public Boolean dynamicAllocation;


    public static final  String CREATE_URL_FORMAT = "%s/sessions";


    @Override
    public String createSession(String sessionName, String executionType) {
        SparkSessionDTO body = new SparkSessionDTO(sessionName,
                hiveFile,dynamicAllocation);
        JSONObject json = HttpUtils.post(String.format(CREATE_URL_FORMAT,url),body);
        return json.getString("id");
    }

    @Override
    public SessionState statusSession(String sessionId) {
        return null;
    }

    @Override
    public String appMasterUI(String sessionId) throws Exception {
        return null;
    }

    @Override
    public StatementResult submit(String statement, String sessionId) {
        return null;
    }

    @Override
    public StatementResult fetch(String statement, String sessionId) {
        return null;
    }


}
