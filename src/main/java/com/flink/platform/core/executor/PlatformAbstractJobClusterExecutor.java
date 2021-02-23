package com.flink.platform.core.executor;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;

import org.apache.flink.yarn.YarnClusterDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ResourceUtils;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Created by 凌战 on 2021/2/19
 */
public class PlatformAbstractJobClusterExecutor<ClusterID,ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PlatformAbstractJobClusterExecutor.class);

    private final ClientFactory clusterClientFactory;

    protected ExecutionContext executionContext;

    public PlatformAbstractJobClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }

    @Override
    public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration, ClassLoader classLoader) throws Exception {

        final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline,configuration);


        //jobGraph.addJars();
        // todo jobGraph 添加自定义方法jar包

        try(final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration) ){

            final ExecutionConfigAccessor configAccessor = ExecutionConfigAccessor.fromConfiguration(configuration);
            final ClusterSpecification clusterSpecification = clusterClientFactory.getClusterSpecification(configuration);

            final ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor
                    .deployJobCluster(clusterSpecification, jobGraph, configAccessor.getDetachedMode());
            LOG.info("Job has been submitted with JobID " + jobGraph.getJobID());

            return CompletableFuture.completedFuture(
                    new ClusterClientJobClientAdapter<>(clusterClientProvider, jobGraph.getJobID(),classLoader));
        }
    }
}
