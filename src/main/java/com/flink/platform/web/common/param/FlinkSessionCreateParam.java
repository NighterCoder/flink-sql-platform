package com.flink.platform.web.common.param;

import lombok.Data;

import java.util.Map;


/***
 * 创建Flink Session的参数配置类
 */
@Data
public class FlinkSessionCreateParam {

    private String sessionName;

    private String planner;

    private String executionType;

    private Map<String,String> properties;

}
