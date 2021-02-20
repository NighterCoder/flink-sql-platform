package com.flink.platform.web.controller;

import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.Result;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.service.FlinkJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * Flink 任务执行Controller
 * 包括 1.jar包提交执行; 2.flink sql提交执行
 * ...
 */
@RestController
@RequestMapping("/api/v1/flink")
@Slf4j
public class FlinkJobController {

    @Autowired
    private FlinkJobService flinkJobService;

    /**
     * 创建Session
     * @param param 查询参数(其中executionType是必填)
     * @return SessionId
     */
    @PostMapping("/session/create")
    public Result createSession(@RequestBody FlinkSessionCreateParam param) {
        String sessionId = flinkJobService.createSession(param);
        return Result.success(sessionId);
    }

    /**
     * 查询Session状态
     * @param sessionId sessionId
     * @return SessionState
     */
    @GetMapping("/session/status")
    public Result status(String sessionId) {
        SessionState state = flinkJobService.sessionHeartBeat(sessionId);
        return Result.success(state);
    }

    /**
     * Flink中只有insert和select才存在jobId,其他的声明语句都可以理解返回
     * @param sql sql语句
     * @param sessionId sessionId
     * @return StatementResult
     */
    @PostMapping("/session/sql/submit")
    public Result submit(String sql,String sessionId) {
        // 1.SQL以分号作为分隔符,形成多个执行SQL

        // 2.执行SQL
        // todo for循环
        // flinkJobService.submit(sql,sessionId);
        return null;
    }


}
