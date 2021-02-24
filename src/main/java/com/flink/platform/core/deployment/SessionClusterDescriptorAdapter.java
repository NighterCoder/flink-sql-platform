package com.flink.platform.core.deployment;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;

/**
 * Created by 凌战 on 2021/2/19
 */
public class SessionClusterDescriptorAdapter<ClusterID> extends ClusterDescriptorAdapter<ClusterID> {

    public SessionClusterDescriptorAdapter(ExecutionContext<ClusterID> executionContext, Configuration configuration, String sessionId, JobID jobId) {
        super(executionContext, configuration, sessionId, jobId);
    }

    @Override
    public boolean isGloballyTerminalState() {
        JobStatus jobStatus = getJobStatus();
        return jobStatus.isGloballyTerminalState();
    }
}
