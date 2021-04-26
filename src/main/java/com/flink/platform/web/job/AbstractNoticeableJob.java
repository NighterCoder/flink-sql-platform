package com.flink.platform.web.job;

import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;

/**
 * 抽象告警类:
 * 主要封装了告警方法
 */
public abstract class AbstractNoticeableJob {


    protected void notice(String taskName, String errorType) {

    }


    protected void notice(NodeExecuteHistory nodeExecuteHistory,String errorType){

    }




}
