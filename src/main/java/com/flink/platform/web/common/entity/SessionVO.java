package com.flink.platform.web.common.entity;

import com.flink.platform.web.common.enums.SessionState;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class SessionVO {
    private String id;
    private SessionState state;
    private String url;
}
