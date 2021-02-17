package com.flink.platform;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
public class PlatformApplication {

    public static void main(String[] args) {
        log.info("##########platform服务开始启动############");
        SpringApplication.run(PlatformApplication.class, args);
        log.info("##########platform服务启动完毕############");
    }

}
