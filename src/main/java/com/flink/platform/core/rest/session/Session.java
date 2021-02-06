package com.flink.platform.core.rest.session;

import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.operation.JobOperation;
import org.apache.flink.api.common.JobID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Similar to HTTP Session, which could maintain user identity and store user-specific data
 * during multiple request/response interactions between a client and the gateway server.
 */
public class Session {

    private static final Logger LOG = LoggerFactory.getLogger(Session.class);

    private final SessionContext context;
    private final String sessionId;

    private long lastVisitedTime;

    private final Map<JobID, JobOperation> jobOperations;

    public Session(SessionContext context) {
        this.context = context;
        this.sessionId = context.getSessionId();

        this.lastVisitedTime = System.currentTimeMillis();

        this.jobOperations = new ConcurrentHashMap<>();
    }




}
