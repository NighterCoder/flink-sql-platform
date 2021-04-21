package com.flink.platform.web.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 加载yarn相关配置属性,定义在deer.properties中
 * 以deer.yarn为前缀
 *
 * 1. 普通应用内存上限
 * 2. 白名单应用,不限制内存
 * ...
 *
 * Created by 凌战 on 2021/4/21
 */
@Component
@ConfigurationProperties(prefix = "deer.yarn") //其中locations属性不可使用,在1.4版本后被去掉了,使用@PropertySource
@PropertySource(value = "classpath:deer.properties")
public class YarnConfig {

    /**
     * 普通应用可申请的内存上限，单位: MB
     * 为0禁用检查
     */
    private Integer appMemoryThreshold = 0;

    /**
     * 白名单应用,可申请内存无限制
     */
    private List<String> appWhiteList = new ArrayList<>();

    public Integer getAppMemoryThreshold() {
        return appMemoryThreshold;
    }

    public void setAppMemoryThreshold(Integer appMemoryThreshold) {
        this.appMemoryThreshold = appMemoryThreshold;
    }

    public List<String> getAppWhiteList() {
        return appWhiteList;
    }

    public void setAppWhiteList(List<String> appWhiteList) {
        this.appWhiteList = appWhiteList;
    }
}
