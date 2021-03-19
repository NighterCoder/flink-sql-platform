package com.flink.platform.web.manager;

import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.enums.SessionState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SparkSessionManager implements SessionManager {


    @Override
    public String createSession(String sessionName, String executionType) {
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

    @Override
    public StatementResult submit(String statement, String sessionId) {
        return null;
    }

    @Override
    public StatementResult fetch(String statement, String sessionId) {
        return null;
    }


}
