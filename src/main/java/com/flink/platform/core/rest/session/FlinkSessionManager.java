package com.flink.platform.core.rest.session;

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.context.DefaultContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.*;

/**
 * Flink Session Manager
 */
public class FlinkSessionManager {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSessionManager.class);

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

    public void open() {
        if (checkInterval > 0 && idleTimeout > 0) {
            executorService = Executors.newSingleThreadScheduledExecutor();
            timeoutCheckerFuture = executorService.scheduleAtFixedRate(() -> {
                LOG.info("Start to remove expired session, current session count: {}", sessions.size());
                for(Map.Entry<String,Session> entry:sessions.entrySet()){
                    String sessionId=entry.getKey();
                    Session session=entry.getValue();

                }

            }, checkInterval, checkInterval, TimeUnit.MILLISECONDS);


        }
    }


    /**
     * 判定当前Session是否过期
     * @param session 会话
     */
    private boolean isSessionExpired(Session session){
        if (idleTimeout > 0){
            return (System.currentTimeMillis() - session.getLastVisitedTime()) > idleTimeout;
        }else{
            return false;
        }
    }

}
