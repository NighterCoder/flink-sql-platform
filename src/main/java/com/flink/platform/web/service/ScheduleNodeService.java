package com.flink.platform.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobNode;

/**
 * Created by 凌战 on 2021/4/22
 */
public interface ScheduleNodeService extends IService<ScheduleNode> {

    String validate(SchedulingJobNode node);

    boolean execute(ScheduleNode scheduleNode, Monitor monitor);

    /**
     * 生成对应节点的执行实例
     * @param scheduleNode 执行节点
     * @param scheduleSnapshot
     * @param scheduleInstanceId
     * @param generateStatus
     * @return
     */
    NodeExecuteHistory generateHistory(ScheduleNode scheduleNode, ScheduleSnapshot scheduleSnapshot,
                                       String scheduleInstanceId, int generateStatus);




}
