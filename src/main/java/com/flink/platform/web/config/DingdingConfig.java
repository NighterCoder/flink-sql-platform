package com.flink.platform.web.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

/**
 * 钉钉相关配置
 *
 * Created by 凌战 on 2021/4/22
 */
@Component
@ConfigurationProperties(prefix = "deer.dingding")
@PropertySource(value = "classpath:deer.properties")
public class DingdingConfig {

    /**
     * 是否启动钉钉消息通知
     */
    private boolean enabled = false;

    /**
     * 公共频道监控
     */
    private String watcherToken;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getWatcherToken() {
        return watcherToken;
    }

    public void setWatcherToken(String watcherToken) {
        this.watcherToken = watcherToken;
    }
}
