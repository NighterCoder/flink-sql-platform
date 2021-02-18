package com.flink.platform.core.rest.session;

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.config.entries.ExecutionEntry;
import com.flink.platform.core.context.DefaultContext;
import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.web.common.entity.FetchData;
import com.flink.platform.web.config.FlinkConfProperties;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Flink Session Manager
 */
public class FlinkSessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionManager.class);

    @Autowired
    private FlinkConfProperties flinkConfProperties;

    private final DefaultContext defaultContext;
    private final long idleTimeout;
    private final long checkInterval;
    private final long maxCount;
    private final Map<String, Session> sessions;

    private ScheduledExecutorService executorService;
    private ScheduledFuture timeoutCheckerFuture;

    public FlinkSessionManager(DefaultContext defaultContext) {
        this.defaultContext = defaultContext;
        Environment env = defaultContext.getDefaultEnv();
        this.idleTimeout = env.getSession().getIdleTimeout();
        this.checkInterval = env.getSession().getCheckInterval();
        this.maxCount = env.getSession().getMaxCount();
        this.sessions = new ConcurrentHashMap<>();
    }

    //todo
    public void open() {
        if (checkInterval > 0 && idleTimeout > 0) {
            executorService = Executors.newSingleThreadScheduledExecutor();
            timeoutCheckerFuture = executorService.scheduleAtFixedRate(() -> {
                LOG.info("Start to remove expired session, current session count: {}", sessions.size());
                for (Map.Entry<String, Session> entry : sessions.entrySet()) {
                    String sessionId = entry.getKey();
                    Session session = entry.getValue();

                }

            }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);


        }
    }


    /**
     * 执行SQL返回结果
     * @param sql 执行SQL
     * @param sessionId sessionId
     * @return FetchData
     */
    public FetchData submit(String sql, String sessionId){
        // todo 加上超时时间
        if (this.sessions.containsKey(sessionId)) {
            this.sessions.get(sessionId);
        }else{
            throw new SqlPlatformException("当前Session不存在");
        }
        return null;
    }






    /**
     * 创建一个Session
     *
     * @param sessionName
     * @param planner
     * @param executionType
     * @param properties
     */
    public String createSession(
            String sessionName,
            String planner,
            String executionType,
            Map<String, String> properties) {
        checkSessionCount();

        if (StringUtils.isNullOrWhitespaceOnly(sessionName)) {
            sessionName = flinkConfProperties.getSessionName();
        }
        if (StringUtils.isNullOrWhitespaceOnly(planner)) {
            planner = flinkConfProperties.getPlanner();
        }
        if (StringUtils.isNullOrWhitespaceOnly(executionType)) {
            executionType = flinkConfProperties.getExecutionType();
        }

        Map<String, String> newProperties = new HashMap<>(properties);
        newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_PLANNER, planner);
        newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_TYPE, executionType);

        if (executionType.equalsIgnoreCase(ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH)) {
            // for batch mode we ensure that results are provided in materialized form
            newProperties.put(
                    Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE,
                    ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_TABLE);
        } else {
            // for streaming mode we ensure that results are provided in changelog form
            newProperties.put(
                    Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE,
                    ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_CHANGELOG);
        }

        Environment sessionEnv = Environment.enrich(
                defaultContext.getDefaultEnv(), newProperties, Collections.emptyMap());

        String sessionId = SessionID.generate().toHexString();
        SessionContext sessionContext = new SessionContext(sessionName, sessionId, sessionEnv, defaultContext);

        Session session = new Session(sessionContext);
        sessions.put(sessionId, session);

        LOG.info("Session: {} is created. sessionName: {}, planner: {}, executionType: {}, properties: {}.",
                sessionId, sessionName, planner, executionType, properties);

        return sessionId;
    }


    /**
     * 根据SessionId查询指定Session
     * @param sessionId SessionId
     */
    public Session getSession(String sessionId){
        // 底层是ConcurrentHashMap存储,这里不用判断sessionId是否存在,直接catch住
        if (this.sessions.containsKey(sessionId)){
            return this.sessions.get(sessionId);
        }
        return null;
    }

    /**
     * 检查Session存在个数
     */
    private void checkSessionCount() {
        if (maxCount <= 0) {
            return;
        }
        if (sessions.size() > maxCount) {
            String msg = String.format(
                    "Failed to create session, the count of active sessions exceeds the max count: %s", maxCount);
            LOG.error(msg);
            throw new SqlPlatformException(msg);
        }
    }

    /**
     * 判定当前Session是否过期
     *
     * @param session 会话
     */
    private boolean isSessionExpired(Session session) {
        if (idleTimeout > 0) {
            return (System.currentTimeMillis() - session.getLastVisitedTime()) > idleTimeout;
        } else {
            return false;
        }
    }

}
