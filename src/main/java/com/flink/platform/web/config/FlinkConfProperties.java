package com.flink.platform.web.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "flink")
public class FlinkConfProperties {

    private String envUrl;
    private String sessionName;
    private String planner;
    private String executionType;

    public String getEnvUrl() {
        return envUrl;
    }
    public void setEnvUrl(String envUrl) {
        this.envUrl = envUrl;
    }

    public String getSessionName() {
        return sessionName;
    }
    public void setSessionName(String sessionName) {
        this.sessionName = sessionName;
    }

    public String getPlanner() {
        return planner;
    }
    public void setPlanner(String planner) {
        this.planner = planner;
    }

    public String getExecutionType() {
        return executionType;
    }
    public void setExecutionType(String executionType) {
        this.executionType = executionType;
    }
}
