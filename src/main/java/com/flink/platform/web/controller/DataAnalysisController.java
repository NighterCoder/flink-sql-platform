package com.flink.platform.web.controller;


import com.flink.platform.web.common.entity.JobSubmitDTO;
import com.flink.platform.web.common.entity.SessionVO;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.enums.ExecuteType;
import com.flink.platform.web.common.enums.SessionType;
import com.flink.platform.web.service.impl.DataAnalysisService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

/**
 * 数据调研/分析页面: 主要是提交SQL获取执行结果,用于前期的数据调研
 * <p>
 * 数据调研/分析场景: flink使用yarn-session模式提交; spark依托Apache Livy的交互式会话模式
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/analysis/")
public class DataAnalysisController {

    @Autowired
    private DataAnalysisService dataAnalysisService;

    /**
     * 获取Session,如果当前存在则直接返回;否则构建新Session返回
     * 另外: 这里Session的维度是针对不同登录用户而言的,即用户隔离
     * <p>
     * 支持的SessionType有Flink和Spark
     * 支持的ExecuteType有Batch和Streaming
     *
     * @param sessionType 支持的计算引擎
     * @param executeType 支持的执行类型
     */
    @GetMapping("session")
    public SessionVO getSession(Integer sessionType, Integer executeType) {
        SessionType st = SessionType.fromCode(sessionType);
        ExecuteType et = ExecuteType.fromCode(executeType);
        return dataAnalysisService.getSession(st, et);
    }

    /**
     * 批量提交执行SQL
     *
     * 该场景下支持
     * 1.Flink: yarn-session模式提交任务,不建议使用yarn-per-job,故前端没有放开
     * 2.Spark: 使用Apache Livy的livy-session模式
     *
     * @param dto 前端参数类
     */
    @PostMapping("submit")
    public StatementResult submit(@RequestBody JobSubmitDTO dto) {
        return dataAnalysisService.submit(dto);
    }


    @PostMapping("fetch")
    public StatementResult fetch(@RequestBody JobSubmitDTO dto){
        return null;
    }



}
