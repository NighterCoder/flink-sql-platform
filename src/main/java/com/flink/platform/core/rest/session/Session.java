package com.flink.platform.core.rest.session;

import com.flink.platform.core.config.entries.ExecutionEntry;
import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.exception.SqlParseException;
import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.operation.JobOperation;
import com.flink.platform.core.operation.SqlCommandParser;
import com.flink.platform.core.rest.result.ResultSet;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
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

    public void touch() {
        lastVisitedTime = System.currentTimeMillis();
    }

    public long getLastVisitedTime() {
        return lastVisitedTime;
    }

    public SessionContext getContext() {
        return context;
    }

    public Tuple2<ResultSet, SqlCommandParser.SqlCommand> runStatement(String statement){
        // TODO: This is a temporary fix to avoid NPE.
        //  In SQL gateway, TableEnvironment is created and used by different threads, thus causing this problem.
        RelMetadataQuery.THREAD_PROVIDERS
                .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));

        LOG.info("Session: {}, run statement: {}", sessionId, statement);
        boolean isBlinkPlanner = context.getExecutionContext().getEnvironment().getExecution().getPlanner()
                .equalsIgnoreCase(ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);

        SqlCommandParser.SqlCommandCall call;
        try{
            Optional<SqlCommandParser.SqlCommandCall> callOpt=SqlCommandParser.parse(statement,isBlinkPlanner);
            if (!callOpt.isPresent()){
                LOG.error("Session: {}, Unknown statement: {}", sessionId, statement);
                throw new SqlPlatformException("Unknown statement: " + statement);
            }else{
                /**
                 * 第一个参数是枚举类型SqlCommand: 比如SELECT INSERT_INTO
                 * SqlCommandParser.SqlCommand command;
                 * 第二个参数是当前命令执行需要的参数
                 * String[] operands;
                 */
                call = callOpt.get();
            }
        }catch (SqlParseException e) {
            LOG.error("Session: {}, Failed to parse statement: {}", sessionId, statement);
            throw new SqlPlatformException(e.getMessage(), e.getCause());
        }

        // 工厂模式创建对应的Operation
        // OperationFa


        return null;

    }


}
