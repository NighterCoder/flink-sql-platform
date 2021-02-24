package com.flink.platform;

import lombok.extern.slf4j.Slf4j;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Objects;

@Slf4j
@SpringBootApplication
//扫描对应Mapper接口所在的包路径
@MapperScan("com.flink.platform.web.mapper")
public class PlatformApplication {

    public static void main(String[] args) {
        log.info("##########platform服务开始启动############");

        System.setProperty("HADOOP_USER_NAME", "hdfs");

        SpringApplication.run(PlatformApplication.class, args);
        log.info("##########platform服务启动完毕############");
    }

}
