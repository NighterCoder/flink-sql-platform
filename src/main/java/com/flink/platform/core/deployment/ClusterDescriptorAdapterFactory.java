package com.flink.platform.core.deployment;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *
 * 工厂模式 根据 execution.target  来决定创建哪一类 ClusterDescriptorAdapter
 *
 * Created by 凌战 on 2021/2/19
 */
public class ClusterDescriptorAdapterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterDescriptorAdapterFactory.class);


    public static <ClusterID> ClusterDescriptorAdapter<ClusterID> create(
            ExecutionContext<ClusterID> executionContext,
            Configuration configuration,
            String sessionId,
            JobID jobId){

        ClusterDescriptorAdapter<ClusterID> clusterDescriptorAdapter;

        if (executionContext.getEnvironment().getExecution().inYarnPerJob()){
            LOG.info("adapter for yarn per job...");
            clusterDescriptorAdapter= new YarnPerJobClusterDescriptorAdapter<>(
                    executionContext,
                    configuration,
                    sessionId,
                    jobId);
        }else{
            LOG.info("adapter for yarn session...");
            clusterDescriptorAdapter = new SessionClusterDescriptorAdapter<>(
                    executionContext,
                    configuration,
                    sessionId,
                    jobId);
        }

        return clusterDescriptorAdapter;
    }



}
