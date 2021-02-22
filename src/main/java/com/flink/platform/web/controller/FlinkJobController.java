package com.flink.platform.web.controller;

import com.flink.platform.web.common.Result;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.service.FlinkJobService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

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
     *
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
     *
     * @param sessionId sessionId
     * @return SessionState
     */
    @GetMapping("/session/status")
    public Result status(String sessionId) {
        SessionState state = flinkJobService.sessionHeartBeat(sessionId);
        return Result.success(state);
    }


    /**
     * todo 目前jar包支持流处理,批处理定时调度待完善
     * 上传Jar包,并且保存到数据库
     *
     * @param jar jar包文件
     */
    @PostMapping("/jar/upload")
    public void upload(@RequestParam(value = "jar") MultipartFile jar) {

    }


}
