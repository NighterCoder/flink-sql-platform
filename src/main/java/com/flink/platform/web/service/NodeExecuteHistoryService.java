package com.flink.platform.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;

/**
 * Created by 凌战 on 2021/4/22
 */
public interface NodeExecuteHistoryService extends IService<NodeExecuteHistory> {

    void missingScheduling(NodeExecuteHistory nodeExecuteHistory);

    /**
     * 监控启动: 监控任务没有scheduleId,只有一个执行节点
     *
     * @param scheduleNodeId 调度节点id
     */
    NodeExecuteHistory findNoScheduleLatestByNodeId(Integer scheduleNodeId);


    boolean execTimeout(NodeExecuteHistory nodeExecuteHistory);


}
