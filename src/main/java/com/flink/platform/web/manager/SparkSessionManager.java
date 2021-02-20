package com.flink.platform.web.manager;

import com.flink.platform.web.common.enums.SessionState;

import java.util.Map;

public class SparkSessionManager implements SessionManager {


    @Override
    public String createSession(String sessionName, String planner, String executionType, Map<String, String> properties) {
        return null;
    }

    @Override
    public SessionState statusSession(String sessionId) {
        return null;
    }

    @Override
    public String appMasterUI(String sessionId) throws Exception {
        return null;
    }


}
