package com.flink.platform.core.rest.session;

import com.flink.platform.core.config.entries.ExecutionEntry;
import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.context.SessionContext;
import com.flink.platform.core.exception.SqlParseException;
import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.operation.*;
import com.flink.platform.core.operation.version.FlinkShims;
import com.flink.platform.core.operation.version.FlinkVersion;
import com.flink.platform.core.rest.result.ResultSet;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.planner.plan.metadata.FlinkDefaultRelMetadataProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Similar to HTTP Session, which could maintain user identity and store user-specific data
 * during multiple request/response interactions between a client and the gateway server.
 *
 * todo 改造,支持不同的Parser来对应不同的语义解析
 *
 *
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

    public Tuple2<ResultSet, SqlCommandParserV2.SqlCommand> runStatement(String statement){
        // TODO: This is a temporary fix to avoid NPE.
        //  In SQL gateway, TableEnvironment is created and used by different threads, thus causing this problem.
        RelMetadataQuery.THREAD_PROVIDERS
                .set(JaninoRelMetadataProvider.of(FlinkDefaultRelMetadataProvider.INSTANCE()));

        LOG.info("Session: {}, run statement: {}", sessionId, statement);

        // 不需要Planner
        boolean isBlinkPlanner = context.getExecutionContext().getEnvironment().getExecution().getPlanner()
                .equalsIgnoreCase(ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);

        SqlCommandParserV2.SqlCommandCall call;
        try{

            // todo 代码改造,适应新版本的SqlCommandParser,FlinkShims当前只是适配1.12版本,后续需要加版本号判断
            FlinkVersion flinkVersion =  new FlinkVersion(EnvironmentInformation.getVersion());
            FlinkShims flinkShims = FlinkShims.getInstance(flinkVersion, new Properties());

            // 获取TableEnvironment
            ExecutionContext executionContext = this.context.getExecutionContext();
            SqlCommandParserV2 sqlCommandParser = new SqlCommandParserV2(flinkShims,executionContext.getTableEnvironment());

            // 旧代码  @Deprecated
            // Optional<SqlCommandParser.SqlCommandCall> callOpt=SqlCommandParser.parse(statement,isBlinkPlanner);

            Optional<SqlCommandParserV2.SqlCommandCall> callOpt = sqlCommandParser.parse(statement);


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
        } catch (Exception e) {
            throw new SqlPlatformException(e.getMessage(), e.getCause());
        }

        // 工厂模式创建对应的Operation
        Operation operation = OperationFactory.createOperation(call,context);
        // 执行对应的命令
        ResultSet resultSet = operation.execute();


        // JobOperation 会提交任务到Flink集群,存在JobId
        if (operation instanceof JobOperation){
            JobOperation jobOperation = (JobOperation) operation;
            jobOperations.put(jobOperation.getJobId(),jobOperation);
        }
        return Tuple2.of(resultSet,call.command);
    }

    /**
     * 只有具备JobId的operation才可以获取结果
     * @param jobId
     * @param token
     * @param maxFetchSize
     */
    public Optional<ResultSet> getJobResult(JobID jobId,long token,int maxFetchSize){
        LOG.info("Session: {}, get result for job: {}, token: {}, maxFetchSize: {}",
                sessionId, jobId, token, maxFetchSize);
        return getJobOperation(jobId).getJobResult(token,maxFetchSize);
    }

    private JobOperation getJobOperation(JobID jobId) throws SqlPlatformException {
        JobOperation jobOperation = jobOperations.get(jobId);
        if (jobOperation == null) {
            String msg = String.format("Job: %s does not exist in current session: %s.", jobId, sessionId);
            LOG.error(msg);
            throw new SqlPlatformException(msg);
        } else {
            return jobOperation;
        }
    }

}
