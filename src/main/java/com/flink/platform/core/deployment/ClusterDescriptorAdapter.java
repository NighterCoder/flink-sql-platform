package com.flink.platform.core.deployment;

import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.exception.SqlExecutionException;
import com.flink.platform.core.exception.SqlPlatformException;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * Adapter to handle kinds of job actions (eg. get job status or cancel job) based on execution.target
 *
 * 目前支持yarn-session和yarn-per-job模式
 */

public abstract class ClusterDescriptorAdapter<ClusterID> {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterDescriptorAdapter.class);
    private static final int DEFAULT_TIMEOUT_SECONDS=30;

    protected final ExecutionContext<ClusterID> executionContext;
    // Only used for logging
    private final String sessionId;
    // jobId is not null only after job is submitted
    protected final JobID jobId;
    protected final Configuration configuration;
    protected final ClusterID clusterID;

    public ClusterDescriptorAdapter(
        ExecutionContext<ClusterID> executionContext,
        Configuration configuration,
        String sessionId,
        JobID jobId) {
        this.executionContext = executionContext;
        this.sessionId = sessionId;
        this.jobId = jobId;
        this.configuration = configuration;
        this.clusterID=executionContext.getClusterClientFactory().getClusterId(configuration);
    }


    /**
     * The reason of using ClusterClient instead of JobClient to retrieve a cluster is
     * the JobClient can't know whether the job is finished on yarn-per-job mode.
     *
     *
     * @param executionContext
     * @param jobId
     * @param sessionId
     * @param function
     * @param <R>
     */
    protected <R> R bridgeClientRequest(
            ExecutionContext<ClusterID> executionContext,
            JobID jobId,
            String sessionId,
            Function<ClusterClient<?>,R> function) {
        if (this.clusterID==null){
            LOG.error("Session: {}. Cluster information don't exist.", sessionId);
            throw new IllegalStateException("Cluster information don't exist.");
        }
        // stop Flink Job
        try (final ClusterDescriptor<ClusterID> clusterDescriptor =
                     executionContext.createClusterDescriptor(configuration)) {
            try (ClusterClient<ClusterID> clusterClient =
                         clusterDescriptor.retrieve(this.clusterID).getClusterClient()) {
                // retrieve existing cluster
                return function.apply(clusterClient);
            } catch (Exception e) {
                LOG.error(
                        String.format("Session: %s, job: %s. Could not retrieve or create a cluster.", sessionId, jobId),
                        e);
                throw new SqlExecutionException("Could not retrieve or create a cluster.", e);
            }
        } catch (SqlExecutionException e) {
            throw e;
        } catch (Exception e) {
            LOG.error(
                    String.format("Session: %s, job: %s. Could not locate a cluster.", sessionId, jobId), e);
            throw new SqlExecutionException("Could not locate a cluster.", e);
        }
    }


    /**
     * 返回Flink Job的执行状态
     * Returns the status of the flink job.
     */
    public JobStatus getJobStatus(){
        if (jobId == null) {
            LOG.error("Session: {}. No job has been submitted. This is a bug.", sessionId);
            throw new IllegalStateException("No job has been submitted. This is a bug.");
        }
        return bridgeClientRequest(this.executionContext, jobId, sessionId, clusterClient -> {
            try {
                return clusterClient.getJobStatus(jobId).get(DEFAULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.error(String.format("Session: %s. Failed to fetch job status for job %s", sessionId, jobId), e);
                throw new SqlPlatformException("Failed to fetch job status for job " + jobId, e);
            }
        });
    }







}








