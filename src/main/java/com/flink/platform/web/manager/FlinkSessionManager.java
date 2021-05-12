package com.flink.platform.web.manager;

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.config.entries.ExecutionEntry;
import com.flink.platform.core.context.DefaultContext;
import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.operation.SqlCommandParser;
import com.flink.platform.core.operation.SqlCommandParserV2;
import com.flink.platform.core.rest.result.ResultSet;
import com.flink.platform.core.rest.session.Session;
import com.flink.platform.core.rest.session.SessionID;
import com.flink.platform.web.common.entity.FetchData;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.enums.SessionType;
import com.flink.platform.web.common.enums.StatementState;
import com.flink.platform.web.config.FlinkConfProperties;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
public class FlinkSessionManager implements SessionManager {

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
     * Flink只有在insert和select才存在jobId
     * <p>
     * 执行SQL返回结果
     * 1. 如果返回JobId,则需要继续执行
     * 2. 如果返回JobId还不存在,则执行结束
     *
     * @param statement 执行sql
     * @param sessionId sessionId
     * @return StatementResult
     */
    public StatementResult submit(String statement, String sessionId) {
        StatementResult res = new StatementResult();
        res.setStatement(statement);

        // todo 加上超时时间
        if (this.sessions.containsKey(sessionId)) {
            Session session = this.sessions.get(sessionId);
            Tuple2<ResultSet, SqlCommandParserV2.SqlCommand> result = session.runStatement(statement);
            ResultSet resultSet = result.f0;
            // SQL类型,SELECT or INSET ...
            String statementType = result.f1.name();

            // 根据statementType不同,结果返回的也不一样
            // 策略模式
            ResultHandlerEnum handlerEnum = ResultHandlerEnum.from(statementType);
            FetchData fetchData = handlerEnum.handle(resultSet);

            if (StringUtils.isNotBlank(fetchData.getJobId())) {
                res.setJobId(fetchData.getJobId());
                res.setState(StatementState.RUNNING);
            } else {
                res.setJobId(String.valueOf(System.currentTimeMillis()));
                res.setState(StatementState.DONE);
                res.setColumns(fetchData.getColumns());
                res.setRows(fetchData.getRows());
            }

            res.setEnd(System.currentTimeMillis());
            return res;

        } else {
            throw new SqlPlatformException("当前Session不存在");
        }
    }

    @Override
    public StatementResult fetch(String statement, String sessionId) {
        return null;
    }

    @Override
    public Session getSession(String sessionId) {
        if (this.sessions.containsKey(sessionId)){
            return this.sessions.get(sessionId);
        }
        return null;
    }

    /**
     * 获取任务执行结果
     *
     * @param sessionId session标识
     * @param jobId     jobId
     * @param token     token
     */
    public FetchData getJobResult(String sessionId, String jobId, Long token, SessionType sessionType) {
        if (this.sessions.containsKey(sessionId)) {
            Session session = this.sessions.get(sessionId);
            JobID jobID = JobID.fromHexString(jobId);

            //批处理与实时处理不一样
            session.getJobResult(jobID, token, flinkConfProperties.getMaxFetchSize());
            return null;
        } else {
            throw new SqlPlatformException("当前Session不存在");
        }
    }


    /**
     * 创建一个Session
     *
     * @param sessionName
     * @param executionType
     */
    public String createSession(String sessionName, String executionType) {
        checkSessionCount();

        if (StringUtils.isBlank(sessionName)) {
            sessionName = flinkConfProperties.getSessionName();
        }
        String planner = flinkConfProperties.getPlanner();
        if (StringUtils.isBlank(executionType)) {
            executionType = flinkConfProperties.getExecutionType();
        }

        Map<String, String> newProperties = new HashMap<>();
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
                sessionId, sessionName, planner, executionType, newProperties);

        return sessionId;
    }

    /**
     * 根据SessionId查询指定Session
     *
     * @param sessionId SessionId
     */
    public SessionState statusSession(String sessionId) {
        // 底层是ConcurrentHashMap存储,这里不用判断sessionId是否存在,直接catch住
        if (this.sessions.containsKey(sessionId)) {
            return SessionState.RUNNING;
        }
        return SessionState.NONE;
    }

    /**
     * 根据sessionId获取Application Url
     *
     * @param sessionId
     */
    @Override
    public String appMasterUI(String sessionId) {
        if (this.sessions.containsKey(sessionId)) {
            Session session = this.sessions.get(sessionId);
            // 如何根据Session获取Application Url
            ExecutionContext executionContext = session.getContext().getExecutionContext();
            ApplicationId applicationId = (ApplicationId) executionContext.getClusterClientFactory().getClusterId(executionContext.getFlinkConfig());
            if (applicationId != null) {
                return applicationId.toString();
            }
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
