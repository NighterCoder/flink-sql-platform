package com.flink.platform.web.service;

import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.web.common.entity.FetchData;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.manager.FlinkSessionManager;
import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Map;

@Service
public class FlinkJobService {

    @Autowired
    private FlinkSessionManager sessionManager;

    /**
     * 创建Session
     * @param param 创建Session的可选参数
     */
    public String createSession(FlinkSessionCreateParam param) {
        String sessionName = param.getSessionName();
        String planner = param.getPlanner();
        String executionType = param.getExecutionType();
        Map<String, String> properties = param.getProperties();
        if (properties == null) {
            properties = Collections.emptyMap();
        }
        String sessionId;
        try {
            sessionId = sessionManager.createSession(sessionName, planner, executionType, properties);
        } catch (Exception e) {
            throw new SqlPlatformException(e.getMessage());
        }
        return sessionId;
    }

    /**
     * 查询指定Session的状态
     * @param sessionId SessionId
     */
    public Session sessionHeartBeat(String sessionId){
        return sessionManager.getSession(sessionId);
    }


    /**
     *
     * @param sql 执行SQL
     * @param sessionId sessionId
     * @return StatementResult
     */
    public StatementResult submit(String sql,String sessionId){
        StatementResult result = new StatementResult();
        // jobId is not null only after job is submitted
        FetchData fetchData = sessionManager.submit(sql,sessionId);

    }





}
