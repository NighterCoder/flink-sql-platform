package com.flink.platform.web.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "flink")
public class FlinkConfProperties {

    private String envUrl;

    public String getEnvUrl() {
        return envUrl;
    }

    public void setEnvUrl(String envUrl) {
        this.envUrl = envUrl;
    }
}
