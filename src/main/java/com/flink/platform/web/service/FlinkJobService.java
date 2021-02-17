package com.flink.platform.web.service;

import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.rest.session.FlinkSessionManager;
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
     *
     * @param param
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


}
