package com.flink.platform.core.deployment;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 凌战 on 2021/2/19
 */
public class YarnPerJobClusterDescriptorAdapter<ClusterID> extends ClusterDescriptorAdapter<ClusterID> {

    private static final Logger LOG = LoggerFactory.getLogger(YarnPerJobClusterDescriptorAdapter.class);

    public YarnPerJobClusterDescriptorAdapter(
            ExecutionContext<ClusterID> executionContext,
            Configuration configuration,
            String sessionId,
            JobID jobId) {
        super(executionContext, configuration, sessionId, jobId);
    }

    @Override
    public boolean isGloballyTerminalState() {
        boolean isGloballyTerminalState;
        try {
            JobStatus jobStatus = getJobStatus();
            isGloballyTerminalState = jobStatus.isGloballyTerminalState();
        } catch (Exception e) {
            if (isYarnApplicationStopped(e)) {
                isGloballyTerminalState = true;
            } else {
                throw e;
            }
        }

        return isGloballyTerminalState;
    }

    /**
     * The yarn application is not running when its final status is not UNDEFINED.
     *
     * <p>In this case, it will throw
     * <code>RuntimeException("The Yarn application " + applicationId + " doesn't run anymore.")</code>
     * from retrieve method in YarnClusterDescriptor.java
     */
    private boolean isYarnApplicationStopped(Throwable e) {
        do {
            String exceptionMessage = e.getMessage();
            if (StringUtils.equals(exceptionMessage, "The Yarn application " + clusterID + " doesn't run anymore.")) {
                LOG.info("{} is stopped.", clusterID);
                return true;
            }
            e = e.getCause();
        } while (e != null);
        return false;
    }


}
