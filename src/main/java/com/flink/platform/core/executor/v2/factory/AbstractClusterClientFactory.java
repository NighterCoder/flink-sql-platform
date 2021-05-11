package com.flink.platform.core.executor.v2.factory;

import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessUtils;
import org.apache.flink.runtime.jobmanager.JobManagerProcessUtils;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Created by 凌战 on 2021/5/11
 */
public interface AbstractClusterClientFactory {

    default ClusterSpecification getClusterSpecification(Configuration configuration) {
        checkNotNull(configuration);

        final int jobManagerMemoryMb = JobManagerProcessUtils
                .processSpecFromConfigWithNewOptionToInterpretLegacyHeap(configuration,
                        JobManagerOptions.TOTAL_PROCESS_MEMORY)
                .getTotalProcessMemorySize()
                .getMebiBytes();

        final int taskManagerMemoryMb = TaskExecutorProcessUtils
                .processSpecFromConfig(TaskExecutorProcessUtils.getConfigurationMapLegacyTaskManagerHeapSizeToConfigOption(
                        configuration, TaskManagerOptions.TOTAL_PROCESS_MEMORY
                ))
                .getTotalProcessMemorySize()
                .getMebiBytes();

        int slotsPerTaskManager = configuration.getInteger(TaskManagerOptions.NUM_TASK_SLOTS);

        return new ClusterSpecification.ClusterSpecificationBuilder()
                .setMasterMemoryMB(jobManagerMemoryMb)
                .setTaskManagerMemoryMB(taskManagerMemoryMb)
                .setSlotsPerTaskManager(slotsPerTaskManager)
                .createClusterSpecification();
    }


    abstract ClusterDescriptor createClusterDescriptor(String clusterConfPath, Configuration flinkConfig);
}
