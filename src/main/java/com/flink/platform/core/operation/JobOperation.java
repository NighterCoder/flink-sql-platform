package com.flink.platform.core.operation;

import com.flink.platform.core.rest.result.ResultSet;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.table.operations.Operation;

import java.util.Optional;

/**
 * An operation will submit a flink job, the corresponding command can be SELECT or INSERT.
 */
public interface JobOperation extends Operation {

    /**
     * Returns the job id after submit the job, that means execute method has been called.
     */
    JobID getJobId();

    /**
     * Get a part of job execution result, this method may be called many times to get all result.
     * Returns Optional.empty if no more results need to be returned,
     * else returns ResultSet (even if the data in the ResultSet is empty).
     *
     * <p>The token must be equal to the previous token + 1 or must be same with the previous token.
     * otherwise a SqlGatewayException will be thrown. This method should return the same result for the same token.
     *
     * <p>The size of result data must be less than maxFetchSize if it's is positive value, else ignore it.
     * Note: the maxFetchSize must be same for same token in each call, else a SqlGatewayException will be thrown.
     */
    Optional<ResultSet> getJobResult(long token, int maxFetchSize);

    /**
     * Returns the status of the flink job.
     */
    JobStatus getJobStatus();

    /**
     * Cancel the flink job.
     */
    void cancelJob();

}
