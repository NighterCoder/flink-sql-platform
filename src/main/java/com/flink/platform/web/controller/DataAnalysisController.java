package com.flink.platform.web.controller;


import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.entity.analysis.SessionVO;
import com.flink.platform.web.common.enums.ExecuteType;
import com.flink.platform.web.common.enums.SessionType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 数据调研/分析页面接口
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/analysis/")
public class DataAnalysisController {


    @RequestMapping("session")
    public SessionVO getSession(Integer sessionType, Integer executeType){
        SessionType st = SessionType.fromCode(sessionType);
        ExecuteType et =  ExecuteType.fromCode(executeType);

    }



}
