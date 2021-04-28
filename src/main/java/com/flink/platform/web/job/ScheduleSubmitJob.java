package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleSnapshotService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 调度提交任务: 更新等待父节点的任务节点
 * <p>
 * Created by 凌战 on 2021/4/28
 */
@DisallowConcurrentExecution
public class ScheduleSubmitJob implements Job {

    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;
    @Autowired
    private ScheduleSnapshotService scheduleSnapshotService;


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        /**
         * 状态什么时候更新的????
         */
        List<NodeExecuteHistory> nodeExecuteHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .eq("state", SystemConstants.JobState.WAITING_PARENT_)
                .orderByAsc("create_time")
        );
        for (NodeExecuteHistory nodeExecuteHistory : nodeExecuteHistoryList) {
            ScheduleSnapshot scheduleSnapshot = scheduleSnapshotService.getById(nodeExecuteHistory.getScheduleSnapshotId());
            // 寻找当前节点的父节点
            ScheduleSnapshot.Topology.Node previousNode = scheduleSnapshot.analyzePreviousNode(nodeExecuteHistory.getScheduleTopologyNodeId());

            if (previousNode == null) {
                // 根节点
                // todo 执行节点
            } else {
                // 不是根节点,需要找到父节点的执行状态
                String previousNodeState = getPreviousNodeState(
                        nodeExecuteHistory.getScheduleId(),
                        previousNode,
                        nodeExecuteHistory.getScheduleInstanceId());
                switch (previousNodeState) {
                    case SystemConstants.JobState.SUCCEEDED:
                        /**
                         * 状态为初始化
                         */
                        nodeExecuteHistory.updateState(SystemConstants.JobState.INITED);
                        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
                        // todo 执行节点
                        break;
                    case SystemConstants.JobState.FAILED:
                        nodeExecuteHistory.updateState(SystemConstants.JobState.PARENT_FAILED_);
                        nodeExecuteHistory.setFinishTime(new Date());
                        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
                        // 循环标志子节点为失败情形
                        markNextNodeFailed(scheduleSnapshot, nodeExecuteHistory.getScheduleInstanceId(), nodeExecuteHistory.getScheduleTopologyNodeId());
                        break;
                    case SystemConstants.JobState.RUNNING:
                        break;
                    default:
                }
            }


        }

    }

    /**
     * 查找前一个执行节点执行状态
     *
     * @param scheduleId         调度id
     * @param previousNode       前一个拓扑节点信息
     * @param scheduleInstanceId 调度实例id
     * @return
     */
    private String getPreviousNodeState(Integer scheduleId, ScheduleSnapshot.Topology.Node previousNode,
                                        String scheduleInstanceId) {
        List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .eq("schedule_id", scheduleId)
                .eq("schedule_instance_id", scheduleInstanceId)
                .eq("schedule_topology_node_id", previousNode.id)
        );
        // 正常执行也只有一个元素在列表中
        for (NodeExecuteHistory previousNodeExecuteHistory : nodeExecuteHistories) {
            if (previousNodeExecuteHistory.isRunning()) {
                return SystemConstants.JobState.RUNNING;
            }
            if (SystemConstants.JobState.SUCCEEDED.equals(previousNodeExecuteHistory.getState())) {
                return SystemConstants.JobState.SUCCEEDED;
            }
        }
        // 如果发生了重试
        nodeExecuteHistories = nodeExecuteHistories.stream().
                filter(nodeExecuteHistory -> nodeExecuteHistory.getScheduleRetryNum() != null)
                .collect(Collectors.toList());
        // 重试次数还没有达到
        if (nodeExecuteHistories.size() < previousNode.retries()) {
            return SystemConstants.JobState.RUNNING;
        }
        return SystemConstants.JobState.FAILED;
    }


    private void markNextNodeFailed(ScheduleSnapshot scheduleSnapshot, String scheduleInstanceId, String scheduleTopologyNodeId) {
        Map<String, ScheduleSnapshot.Topology.Node> nextNodeIdToObj = scheduleSnapshot.analyzeNextNode(scheduleTopologyNodeId);
        for (Map.Entry<String, ScheduleSnapshot.Topology.Node> entry : nextNodeIdToObj.entrySet()) {
            NodeExecuteHistory nextNodeExecuteHistory = nodeExecuteHistoryService.getOne(new QueryWrapper<NodeExecuteHistory>()
                    .eq("schedule_id", scheduleSnapshot.getScheduleId())
                    .eq("schedule_instance_id", scheduleInstanceId)
                    .eq("schedule_topology_node_id", entry.getValue().id));
            nextNodeExecuteHistory.updateState(SystemConstants.JobState.PARENT_FAILED_);
            nextNodeExecuteHistory.setFinishTime(new Date());
            nodeExecuteHistoryService.saveOrUpdate(nextNodeExecuteHistory);
            markNextNodeFailed(scheduleSnapshot, scheduleInstanceId, nextNodeExecuteHistory.getScheduleTopologyNodeId());
        }
    }


}