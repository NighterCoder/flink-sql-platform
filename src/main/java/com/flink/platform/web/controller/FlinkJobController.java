package com.flink.platform.web.controller;

import com.flink.platform.web.common.Result;
import com.flink.platform.web.service.FlinkJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Flink 任务执行Controller
 * 包括 1.jar包提交执行; 2.flink sql提交执行
 * ...
 */
@RestController
@RequestMapping("/api/flink")
@Slf4j
public class FlinkJobController {

    @Autowired
    private FlinkJobService flinkJobService;

    @PostMapping("/sql/submit")
    public Result submit(String sql){
        // 1.SQL以分号作为分隔符,形成多个执行SQL

        // 2.执行SQL

        return null;
    }



}
