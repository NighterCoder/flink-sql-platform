package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.mapper.NodeExecuteHistoryMapper;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class NodeExecuteHistoryServiceImpl extends ServiceImpl<NodeExecuteHistoryMapper, NodeExecuteHistory> implements NodeExecuteHistoryService {

    /**
     * 执行节点错过调度
     *
     * @param nodeExecuteHistory 调度节点执行实例
     */
    @Override
    public void missingScheduling(NodeExecuteHistory nodeExecuteHistory) {
        nodeExecuteHistory.updateState(SystemConstants.JobState.FAILED);
        nodeExecuteHistory.setErrors("Missing scheduling");
        saveOrUpdate(nodeExecuteHistory);
    }

    /**
     * 根据节点id查找,调度id不存在
     * 根据创建时间倒序
     *
     * @param scheduleNodeId 调度节点id
     */
    @Override
    public NodeExecuteHistory findNoScheduleLatestByNodeId(Integer scheduleNodeId) {
        return getOne(new QueryWrapper<NodeExecuteHistory>()
                .eq("node_id", scheduleNodeId)
                .isNull("schedule_id")
                .orderByDesc("create_time")
        );
    }

    @Override
    public boolean execTimeout(NodeExecuteHistory nodeExecuteHistory) {
        // 倒推出最早的执行时间
        Date ago = DateUtils.addMinutes(new Date(),-nodeExecuteHistory.getTimeout());
        // 执行超时
        Date time;
        /**
         * 正常定时的开始执行时间
         */
        if (nodeExecuteHistory.getStartTime() != null){
            time  = nodeExecuteHistory.getStartTime();
        }else if (nodeExecuteHistory.getScheduleHistoryTime() != null){
            /**
             * 重试的开始执行时间
             */
            time = nodeExecuteHistory.getScheduleHistoryTime();
        }else{
            time = nodeExecuteHistory.getCreateTime();
        }

        return time.compareTo(ago) <= 0;
    }


}
