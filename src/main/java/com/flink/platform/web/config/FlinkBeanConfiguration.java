package com.flink.platform.web.config;

import com.flink.platform.core.config.Environment;
import com.flink.platform.core.context.DefaultContext;
import com.flink.platform.web.manager.FlinkSessionManager;
import com.flink.platform.web.common.util.EnvironmentUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

@Configuration
public class FlinkBeanConfiguration {

    @Autowired
    private FlinkConfProperties flinkConfProperties;

    @Bean
    public FlinkSessionManager flinkSessionManager() {
        try {
            // 初始化DefaultContext
            String envUrlStr = flinkConfProperties.getEnvUrl();
            URL envUrl = new URL(Objects.requireNonNull(this.getClass().getClassLoader().getResource("")).toString()+envUrlStr);
            Environment env= EnvironmentUtil.readEnvironment(envUrl);
            // todo env和dependencies一般是命令行参数指定的
            DefaultContext defaultContext=new DefaultContext(env,null);
            return new FlinkSessionManager(defaultContext);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return null;
    }


}
