package com.flink.platform.core;

import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.executor.PlatformYarnJobClusterExecutor;
import com.flink.platform.core.executor.PlatformYarnSessionClusterExecutor;
import com.flink.platform.web.common.SystemConstants;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * The helper class to deploy a table program on the cluster.
 * Created by 凌战 on 2021/2/19
 */
public class ProgramDeployer {


    private static final Logger LOG = LoggerFactory.getLogger(ProgramDeployer.class);

    private final ExecutionContext executionContext;
    private final Configuration configuration;
    private final Pipeline pipeline;
    private final String jobName;

    public ProgramDeployer(
            ExecutionContext executionContext,
            Configuration configuration,
            String jobName,
            Pipeline pipeline) {
        this.executionContext = executionContext;
        this.configuration = configuration;
        this.pipeline = pipeline;
        this.jobName = jobName;
    }


    /**
     * 根据 execution-target 决定是以哪一种模式运行flink程序
     */
    public CompletableFuture<JobClient> deploy(){
        LOG.info("Submitting job {} for query {}`", pipeline, jobName);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Submitting job {} with configuration: \n{}", pipeline, configuration);
        }

        if (configuration.get(DeploymentOptions.TARGET) == null){
            throw new RuntimeException("No execution.target specified in your configuration file.");
        }

        // todo 待完善
        PipelineExecutor executor;
        if (this.executionContext.getEnvironment().getExecution().inYarnPerJob()){
            LOG.info("in deployer, target = yarn-per-job");
            executor = new PlatformYarnJobClusterExecutor(executionContext, SystemConstants.FLINK_LIB_DIR);
        }else{
            LOG.info("in deployer, target = yarn-session");
            executor = new PlatformYarnSessionClusterExecutor(executionContext);
        }
        try {
            return executor.execute(pipeline, configuration,executionContext.getClassLoader());
        } catch (Exception e) {
            throw new RuntimeException("Could not execute program.", e);
        }
    }



}
