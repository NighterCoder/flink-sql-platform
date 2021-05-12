package com.flink.platform.web.manager;

import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.enums.SessionState;

/**
 * Created by 凌战 on 2021/2/20
 */
public interface SessionManager {

    String createSession(String sessionName, String executionType);

    SessionState statusSession(String sessionId);

    String appMasterUI(String sessionId) throws Exception;

    StatementResult submit(String statement, String sessionId);

    StatementResult fetch(String statement , String sessionId);

    Session getSession(String sessionId);

}
