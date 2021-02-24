package com.flink.platform.web.manager;

import com.flink.platform.core.rest.result.ColumnInfo;
import com.flink.platform.core.rest.result.ResultSet;
import com.flink.platform.web.common.entity.FetchData;
import org.apache.flink.types.Row;


import java.util.List;
import java.util.function.Function;

/**
 * Created by 凌战 on 2021/2/19
 */
public enum ResultHandlerEnum {

    // SELECT 会返回结果
    SELECT((resultSet) -> {
        String jobId = ResultHandlerUtils.getJobID(resultSet).toHexString();
        FetchData data = new FetchData();
        data.setJobId(jobId);
        return data;
    }),

    // SET
    OTHER(ResultHandlerEnum::fetchFormat)
    ;

    private Function<ResultSet, FetchData> function;

    ResultHandlerEnum(Function<ResultSet, FetchData> function) {
        this.function = function;
    }

    public FetchData handle(ResultSet body) {
        return this.function.apply(body);
    }

    public static ResultHandlerEnum from(String statementType){
        try {
            // 其他类型转为other
            return ResultHandlerEnum.valueOf(statementType);
        }catch (Exception e){
            return ResultHandlerEnum.OTHER;
        }
    }


    public static FetchData fetchFormat(ResultSet rs) {
        FetchData data = new FetchData();

        if (rs.getData().size() > 0) {

            List<ColumnInfo> columns = rs.getColumns();
            List<Row> rows = rs.getData();

            for (ColumnInfo columnInfo : columns) {
                data.addColumn(columnInfo.getName());
            }

            for (int i = 0; i < rows.size(); i++) {
                for (int j = 0; j < columns.size(); j++) {
                    data.addRow(i, columns.get(j).getName(), rows.get(i).getField(j));
                }
            }
        }

        return data;
    }
}
