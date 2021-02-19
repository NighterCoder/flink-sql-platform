package com.flink.platform.web.manager;

import com.flink.platform.core.rest.result.ResultSet;
import com.flink.platform.web.common.entity.FetchData;


import java.util.function.Function;

/**
 * Created by 凌战 on 2021/2/19
 */
public enum ResultHandlerEnum {

    SELECT((resultSet) -> {
        String jobId = ResultHandlerUtils.getJobID(resultSet).toHexString();
        FetchData data = new FetchData();
        data.setJobId(jobId);
        return data;
    }),

    ;

    private Function<ResultSet, FetchData> function;

    ResultHandlerEnum(Function<ResultSet, FetchData> function) {
        this.function = function;
    }

    public FetchData handle(ResultSet body) {
        return this.function.apply(body);
    }

    public static ResultHandlerEnum from(String statementType){
        return ResultHandlerEnum.valueOf(statementType);
    }


}
