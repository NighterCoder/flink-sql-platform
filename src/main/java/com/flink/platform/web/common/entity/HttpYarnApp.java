package com.flink.platform.web.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by 凌战 on 2021/4/21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class HttpYarnApp {

    private String id;
    private String name;
    private String user;
    private String queue;
    private String state;
    private String finalStatus;
    private String trackingUrl;
    private String diagnostics;
    private String applicationType;
    private Long startedTime;
    private Long finishedTime;
    private Integer allocatedMB;

}
