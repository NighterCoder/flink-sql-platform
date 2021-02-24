package com.flink.platform.web.manager;

import com.flink.platform.core.rest.result.ResultSet;
import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Either;

/**
 * Created by 凌战 on 2021/2/19
 */
public class ResultHandlerUtils {

    public static JobID getJobID(ResultSet resultSet) {
        if (resultSet.getColumns().size() != 1) {
            throw new IllegalArgumentException("Should contain only one column. This is a bug.");
        } else if (resultSet.getColumns().get(0).getName().equals("job_id")) {
            String jobId = (String) resultSet.getData().get(0).getField(0);
            return JobID.fromHexString(jobId);
        } else {
            throw new IllegalArgumentException("Column name should be job_id. This is a bug.");
        }
    }

    public static Either<JobID, ResultSet> getEitherJobIdOrResultSet(ResultSet resultSet) {
        if (resultSet.getColumns().size() == 1 && resultSet.getColumns().get(0).getName().equals("job_id")) {
            String jobId = (String) resultSet.getData().get(0).getField(0);
            return Either.Left(JobID.fromHexString(jobId));
        } else {
            return Either.Right(resultSet);
        }
    }


}
