package com.flink.platform.web.config;

import com.flink.platform.web.manager.SparkSessionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Created by 凌战 on 2021/2/20
 */
@Configuration
public class SparkBeanConfiguration {

    @Bean
    public SparkSessionManager sparkSessionManager(){
        return new SparkSessionManager();
    }

}
