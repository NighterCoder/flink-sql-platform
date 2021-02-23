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

        //todo windows系统本地调试需要设置下环境变量
        System.setProperty("HADOOP_CONF_DIR", "C:\\resources\\flink-sql-platform\\src\\main\\resources\\dev");

        SpringApplication.run(PlatformApplication.class, args);
        log.info("##########platform服务启动完毕############");
    }

}
