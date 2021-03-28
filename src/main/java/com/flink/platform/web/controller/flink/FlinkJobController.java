package com.flink.platform.web.controller.flink;

import com.flink.platform.web.common.entity.Msg;
import com.flink.platform.web.common.entity.jar.JarJobConf;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.controller.BaseController;
import com.flink.platform.web.service.FlinkJobService;
import com.flink.platform.web.service.JarManagerService;
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
public class FlinkJobController extends BaseController {

    @Autowired
    private FlinkJobService flinkJobService;

    @Autowired
    private JarManagerService jarManagerService;

    /**
     * 创建Session
     *
     * @param param 查询参数(其中executionType是必填)
     * @return SessionId
     */
    @PostMapping("/session/create")
    public Msg createSession(@RequestBody FlinkSessionCreateParam param) {
        String sessionId = flinkJobService.createSession(param);
        return success(sessionId);
    }

    /**
     * 查询Session状态
     *
     * @param sessionId sessionId
     * @return SessionState
     */
    @GetMapping("/session/status")
    public Msg status(String sessionId) {
        SessionState state = flinkJobService.sessionHeartBeat(sessionId);
        return success(state);
    }


    /**
     * todo 1.目前jar包支持流处理,批处理定时调度待完善
     * todo 2.上传之后保存对应的jar包地址
     * <p>
     * 上传Jar包,并且保存到数据库
     *
     * @param jar jar包文件
     */
    @PostMapping("/jar/upload")
    public void upload(@RequestParam(value = "jar") MultipartFile jar) throws Exception {
        jarManagerService.upload(jar);
    }

    /**
     * 提交Jar包
     *
     * @param jarJobConf jar提交参数类
     */
    @PostMapping("/jar/submit")
    public String submit(@RequestBody JarJobConf jarJobConf) throws Exception {
        return flinkJobService.submitJar(jarJobConf);
    }


}
