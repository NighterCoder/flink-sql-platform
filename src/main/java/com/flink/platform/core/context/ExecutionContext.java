package com.flink.platform.core.context;

import com.flink.platform.core.config.Environment;
import org.apache.commons.cli.Options;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.CustomCommandLine;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientServiceLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.delegation.Executor;
import org.apache.flink.table.module.ModuleManager;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.Collections;
import java.util.List;

/**
 * Context for executing table programs. This class caches everything that can be cached across
 * multiple queries as long as the session context does not change. This must be thread-safe as
 * it might be reused across different query submissions.
 *
 * Session Context不变,对应Execution Context不变
 *
 * @param <ClusterID> cluster id
 */
public class ExecutionContext<ClusterID> {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutionContext.class);

    private final Environment environment;
    private final ClassLoader classLoader;

    private final Configuration flinkConfig;
    private final ClusterClientFactory<ClusterID> clusterClientFactory;

    private TableEnvironment tableEnv;
    private ExecutionEnvironment execEnv;
    private StreamExecutionEnvironment streamExecEnv;
    private Executor executor;

    // Members that should be reused in the same session.
    private SessionState sessionState;

    private ExecutionContext(
            Environment environment,
            @Nullable SessionState sessionState,
            List<URL> dependencies,
            Configuration flinkConfig,
            ClusterClientServiceLoader clusterClientServiceLoader,
            Options commandLineOptions,
            List<CustomCommandLine> availableCommandLines) throws FlinkException {
        this.environment = environment;

        this.flinkConfig = flinkConfig;

        // 创建类加载器,这里的类加载器是子类加载优先
        classLoader = ClientUtils.buildUserCodeClassLoader(
                dependencies,
                Collections.emptyList(),
                this.getClass().getClassLoader(),
                flinkConfig
        );




    }

    private void initializeTableEnvironment(@Nullable SessionState sessionState){
        final EnvironmentSettings settings = environment.getExecution().getEnvironmentSettings();
        // Step 0.0 Initialize the table configuration.
        // 将Environment中的属性拿过来,主要是sql-client-default.yml中
        final TableConfig config=new TableConfig();
        // 返回ConfigurationEntry
        environment.getConfiguration().asMap().forEach((k,v)->{
            config.getConfiguration().setString(k,v);
        });
        // 没有可继承的状态
        final boolean noInheritedState = sessionState == null;
        if (noInheritedState){
            // Step 1 Create environment

            // Step 1.0 初始化ModuleManager
            final ModuleManager moduleManager = new ModuleManager();
            // Step 1.1 初始化CatalogManager
            final CatalogManager catalogManager = CatalogManager.newBuilder()
                    .classLoader(classLoader)
                    .config(config.getConfiguration())
                    .defaultCatalog(
                            settings.getBuiltInCatalogName(),
                            new GenericInMemoryCatalog(
                                    settings.getBuiltInCatalogName(),
                                    settings.getBuiltInDatabaseName())
                    ).build();

            // Step 1.2 Initialize the FunctionCatalog if required.
            final FunctionCatalog functionCatalog = new FunctionCatalog(config, catalogManager, moduleManager);
            // Step 1.3 Set up session state.
            this.sessionState = SessionState.of(catalogManager, moduleManager, functionCatalog);


        }

    }

    /**
     * 创建Table执行环境
     * 根据批处理和流处理有所区别
     * @param settings
     * @param config
     * @param catalogManager
     * @param moduleManager
     * @param functionCatalog
     */
    private void createTableEnvironment(
            EnvironmentSettings settings,
            TableConfig config,
            CatalogManager catalogManager,
            ModuleManager moduleManager,
            FunctionCatalog functionCatalog){
        // 流处理: 初始化流处理执行环境;批处理执行环境为null
        if(environment.getExecution().isStreamingPlanner()){
            streamExecEnv = 
        }



    }



    /** Represents the state that should be reused in one session. **/
    public static class SessionState{
        public final CatalogManager catalogManager;
        public final ModuleManager moduleManager;
        public final FunctionCatalog functionCatalog;

        private SessionState(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            this.catalogManager = catalogManager;
            this.moduleManager = moduleManager;
            this.functionCatalog = functionCatalog;
        }

        public static SessionState of(
                CatalogManager catalogManager,
                ModuleManager moduleManager,
                FunctionCatalog functionCatalog) {
            return new SessionState(catalogManager, moduleManager, functionCatalog);
        }



    }

}
