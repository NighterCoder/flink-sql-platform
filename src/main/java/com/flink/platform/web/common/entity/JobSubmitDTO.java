package com.flink.platform.web.common.entity;

import com.flink.platform.web.common.enums.ExecuteType;
import com.flink.platform.web.common.enums.SessionType;
import lombok.Data;


/**
 * Created by 凌战 on 2021/2/20
 */
@Data
public class JobSubmitDTO {
    private String sessionId;
    private String jobId; // 首次提交不存在JobId
    private String sql;
    private Integer sessionType;
    private Integer executeType;
    private Long token;

    public SessionType getSessionType() {
        return SessionType.fromCode(sessionType);
    }

    public ExecuteType getExecuteType() {
        return ExecuteType.fromCode(executeType);
    }
}
