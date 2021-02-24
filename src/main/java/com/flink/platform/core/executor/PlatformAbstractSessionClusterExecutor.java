package com.flink.platform.core.executor;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.ClusterClientFactory;
import org.apache.flink.client.deployment.ClusterClientJobClientAdapter;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Created by 凌战 on 2021/2/19
 */
public class PlatformAbstractSessionClusterExecutor<ClusterID,ClientFactory extends ClusterClientFactory<ClusterID>> implements PipelineExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(PlatformAbstractSessionClusterExecutor.class);

    private final ClientFactory clusterClientFactory;

    protected ExecutionContext executionContext;

    public PlatformAbstractSessionClusterExecutor(@Nonnull final ClientFactory clusterClientFactory) {
        this.clusterClientFactory = checkNotNull(clusterClientFactory);
    }



    @Override
    public CompletableFuture<JobClient> execute(Pipeline pipeline,
                                                Configuration configuration,
                                                ClassLoader classLoader) throws Exception {
        final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, configuration);

        //yarn-session模式,我们需要先找到yarn session集群的application id
        try (final ClusterDescriptor<ClusterID> clusterDescriptor = clusterClientFactory.createClusterDescriptor(configuration)){
            final ClusterID clusterID=clusterClientFactory.getClusterId(configuration);
            checkState(clusterID != null);

            // 获取对应yarn session集群的ClusterClient
            ClusterClientProvider<ClusterID> clusterClientProvider = clusterDescriptor.retrieve(clusterID);
            ClusterClient<ClusterID> clusterClient = clusterClientProvider.getClusterClient();
            return clusterClient.submitJob(jobGraph)
                    .thenApplyAsync(jobID -> (JobClient) new ClusterClientJobClientAdapter<>(
                            clusterClientProvider,
                            jobID,
                            classLoader))
                    .whenComplete((ignored1, ignored2) -> clusterClient.close());

        }
    }
}
