package com.flink.platform.web.common.entity;

import com.flink.platform.web.common.enums.StatementState;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by 凌战 on 2021/2/19
 */
@Data
public class StatementResult {
    private String statement;
    private String jobId;
    private StatementState state;
    private List<Column> columns = new ArrayList<>();
    private List<Map<String, Object>> rows = new ArrayList<>();
    private Long start;
    private Long end;

    public StatementResult() {
        this.start = System.currentTimeMillis();
    }

}
