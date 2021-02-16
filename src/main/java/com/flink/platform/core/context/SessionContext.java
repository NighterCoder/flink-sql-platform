package com.flink.platform.core.context;

import com.flink.platform.core.config.Environment;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.Optional;

/**
 * Context describing current session properties, original properties, ExecutionContext, etc.
 */
public class SessionContext {

    private final String sessionName;
    private final String sessionId;
    private final Environment originalSessionEnv;
    private final DefaultContext defaultContext;
    private ExecutionContext<?> executionContext;

    public SessionContext(
            @Nullable String sessionName,
            String sessionId,
            Environment originalSessionEnv,
            DefaultContext defaultContext) {
        this.sessionName = sessionName;
        this.sessionId = sessionId;
        this.originalSessionEnv = originalSessionEnv;
        this.defaultContext = defaultContext;
        this.executionContext = createExecutionContextBuilder(originalSessionEnv).build();
    }

    public Optional<String> getSessionName(){
        return Optional.ofNullable(sessionName);
    }

    public String getSessionId(){
        return this.sessionId;
    }

    public Environment getOriginalSessionEnv(){
        return this.originalSessionEnv;
    }

    public ExecutionContext<?> getExecutionContext() {
        return executionContext;
    }

    public void setExecutionContext(ExecutionContext<?> executionContext) {
        this.executionContext = executionContext;
    }


    /** Returns ExecutionContext.Builder with given {@link SessionContext} session context. */
    public ExecutionContext.Builder createExecutionContextBuilder(Environment sessionEnv) {
        return ExecutionContext.builder(
                defaultContext.getDefaultEnv(),
                sessionEnv,
                defaultContext.getDependencies(),
                defaultContext.getFlinkConfig(),
                defaultContext.getClusterClientServiceLoader(),
                defaultContext.getCommandLineOptions(),
                defaultContext.getCommandLines());
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionName,sessionId,originalSessionEnv,executionContext);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SessionContext)) {
            return false;
        }

        SessionContext context= (SessionContext) obj;
        return Objects.equals(sessionName, context.sessionName) &&
                Objects.equals(sessionId, context.sessionId) &&
                Objects.equals(originalSessionEnv, context.originalSessionEnv) &&
                Objects.equals(executionContext, context.executionContext);
    }
}
