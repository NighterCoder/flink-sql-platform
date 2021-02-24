package com.flink.platform.web.config;

import com.baomidou.mybatisplus.extension.plugins.PaginationInterceptor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Mybatis-plus的分页配置类
 * 顺便扫描mapper文件夹
 * Created by 凌战 on 2021/2/20
 */
@Configuration
public class MybatisPageConfig {

    @Bean
    public PaginationInterceptor paginationInterceptor(){
        return new PaginationInterceptor();
    }

}
