package com.flink.platform.web.manager;

import com.flink.platform.web.common.entity.FetchData;
import com.flink.platform.web.common.enums.SessionState;

import java.util.Map;

/**
 * Created by 凌战 on 2021/2/20
 */
public interface SessionManager {

    //todo 优化
    String createSession(String sessionName,
                         String planner,
                         String executionType,
                         Map<String, String> properties);

    SessionState statusSession(String sessionId);

    String appMasterUI(String sessionId) throws Exception;

    FetchData submit(String statement, String sessionId);
}
