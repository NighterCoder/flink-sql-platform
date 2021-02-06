package com.flink.platform.core.context;

import com.flink.platform.core.config.Environment;

import javax.annotation.Nullable;

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

}
