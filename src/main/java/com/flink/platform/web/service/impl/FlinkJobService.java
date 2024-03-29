package com.flink.platform.web.service.impl;

import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.executor.PlatformYarnJobClusterExecutor;
import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.jar.JarJobConf;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.manager.FlinkSessionManager;
import com.flink.platform.web.manager.HDFSManager;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.deployment.ClusterRetrieveException;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.*;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;
import org.apache.flink.yarn.configuration.YarnLogConfigUtil;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * Flink定时任务执行Service类
 */
@Service
public class FlinkJobService {

    @Autowired
    private FlinkSessionManager sessionManager;

    @Autowired
    private HDFSManager hdfsManager;


    /**
     * 创建Session
     *
     * @param param 创建Session的可选参数
     */
    public String createSession(FlinkSessionCreateParam param) {
        String sessionName = param.getSessionName();
        String executionType = param.getExecutionType();
        Map<String, String> properties = param.getProperties();
        if (properties == null) {
            properties = Collections.emptyMap();
        }
        String sessionId;
        try {
            sessionId = sessionManager.createSession(sessionName, executionType);
        } catch (Exception e) {
            throw new SqlPlatformException(e.getMessage());
        }
        return sessionId;
    }

    /**
     * 查询指定Session的状态
     *
     * @param sessionId SessionId
     */
    public SessionState sessionHeartBeat(String sessionId) {
        return sessionManager.statusSession(sessionId);
    }

    /**
     * 返回SQL执行的结果
     *
     * @param sql       执行SQL
     * @param sessionId sessionId
     *
     * @return StatementResult
     */
    public StatementResult submit(String sql, String sessionId,NodeExecuteHistory nodeExecuteHistory) {
        StatementResult result = new StatementResult();
        result.setStatement(sql);
        // jobId is not null only after job is submitted

        List<String> list = Arrays.stream(sql.split(";"))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());

        int size = list.size();
        for (int i = 0; i < size; i++) {

            // todo 为什么选择在这里进行血缘解析???? 因为语义的变化可能会在sql中以set table.sql-dialect 来动态变化,只能执行一句来解析一句
            // 每执行一个SQL语句之前先进行血缘分析
            // 1.先获取当前语句执行的Session,从而获取TableEnv
            Session session = sessionManager.getSession(sessionId);
            SqlDialect sqlDialect = session.getContext().getExecutionContext().getTableEnvironment().getConfig().getSqlDialect();

            // 只返回最后一个语句的执行结果
            if (i == size - 1) {
                return sessionManager.submit(list.get(i), sessionId);
            }
            sessionManager.submit(list.get(i), sessionId);
        }
        return new StatementResult();
    }

    /**
     * Yarn 模式提交JAR包
     * todo 支持Per Job模式和Session模式
     * 提交Jar包
     *
     * @param jarJobConf 参数类
     */
    public String submitJar(JarJobConf jarJobConf) throws Exception {
        // 1. 先去下载jar包
        String jarPath = jarJobConf.getUserJarPath();
        String dest = "/tmp/" + jarJobConf.getUserJarName();
        hdfsManager.download(jarPath, dest);
        // 构建jar File
        File jar = new File(dest);

        // 2. load the global configuration 加载flink-conf.yaml构成Configuration
        String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        configuration.set(
                DeploymentOptions.TARGET,
                YarnDeploymentTarget.PER_JOB.getName());


        // 3. 构建PackagedProgram
        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder()
                        .setJarFile(jar)
                        .setUserClassPaths(jarJobConf.parseUserClassPaths())
                        .setEntryPointClassName(jarJobConf.getEntryClass())
                        .setConfiguration(configuration)
                        .setSavepointRestoreSettings(jarJobConf.parseSavepointRestoreSettings())
                        .setArguments(jarJobConf.getArgs())
                        .build();
        // 开启日志,需要在目录下配置log4j.properties
        YarnLogConfigUtil.setLogConfigFileInConfig(configuration, configurationDirectory);

        // 4. 加载jar包, 用户jar包
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.JARS,
                packagedProgram.getJobJarAndDependencies(),
                URL::toString
        );

        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.CLASSPATHS,
                packagedProgram.getClasspaths(),
                URL::toString
        );

        // 5. 构建pipeline
        Pipeline pipeline = this.wrapClassLoader(packagedProgram.getUserCodeClassLoader(), () -> {
            try {
                return PackagedProgramUtils.
                        getPipelineFromProgram(
                                packagedProgram,
                                configuration,
                                10,
                                false);
            } catch (ProgramInvocationException e) {
                e.printStackTrace();
                return null;
            }
        });

        //todo 流处理在提交执行的时候分析血缘关系,而定时批处理则在创建任务的时候分析血缘关系
        FlinkLineageAnalysisUtils.streamJarLineageAnalysis((StreamGraph) pipeline);


        // yarn-per-job模式
        return new PlatformYarnJobClusterExecutor(null, SystemConstants.FLINK_LIB_DIR).
                execute(pipeline, configuration, packagedProgram.getUserCodeClassLoader()).get().getJobID().toString();

    }


    /**
     * 停止正在运行的任务
     *
     * @param jarJobId 任务id
     */
    public void stopJobWithSavepoint(Integer jarJobId) throws ExecutionException, InterruptedException, ClusterRetrieveException {
        // todo 根据jarJobId 查询数据库返回 JarJobConf
        JarJobConf jarJobConf = null;
        String applicationIdStr = jarJobConf.getApplicationId();
        ApplicationId applicationId = ConverterUtils.toApplicationId(applicationIdStr);


        JobID jobID = jarJobConf.parseJobId();

        String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);
        YarnClusterClientFactory clusterClientFactory = new YarnClusterClientFactory();

        YarnClusterDescriptor clusterDescriptor = clusterClientFactory
                .createClusterDescriptor(
                        configuration);

        ClusterClient<ApplicationId> clusterClient = clusterDescriptor.retrieve(applicationId).getClusterClient();
        CompletableFuture<String> completableFuture = clusterClient.stopWithSavepoint(
                jobID,
                true,
                jarJobConf.getSavepointPath());

        String savepoint = completableFuture.get();

        // todo 将savepoint更新到数据库


    }


    public void submitUdfJar() {

    }


    private <R> R wrapClassLoader(ClassLoader classLoader, Supplier<R> supplier) {
        try (TemporaryClassLoaderContext tmpCl = TemporaryClassLoaderContext.of(classLoader)) {
            return supplier.get();
        }
    }


}
