package com.flink.platform.web.job;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleSnapshotService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

/**
 * Created by 凌战 on 2021/4/28
 */
public abstract class AbstractRetryableJob extends AbstractNoticeableJob {

    @Autowired
    protected NodeExecuteHistoryService nodeExecuteHistoryService;
    @Autowired
    protected ScheduleSnapshotService scheduleSnapshotService;

    /**
     * 重试当前节点
     *
     * @param nodeExecuteHistory
     * @param errorType
     */
    protected void retryCurrentNode(NodeExecuteHistory nodeExecuteHistory, String errorType) {
        notice(nodeExecuteHistory, errorType);
        /**
         * 查看当前是不是可以重试的状态
         */
        boolean retryable = nodeExecuteHistory.getScheduleId() != null &&
                (nodeExecuteHistory.getScheduleHistoryMode() == null || nodeExecuteHistory.getScheduleHistoryMode().equals(SystemConstants.HistoryMode.RETRY)) &&
                !"UNKNOWN".equals(nodeExecuteHistory.getJobFinalStatus());
        if (retryable){
            ScheduleSnapshot scheduleSnapshot = scheduleSnapshotService.getById(nodeExecuteHistory.getScheduleSnapshotId());
            /**
             * 获取执行节点,进一步获取重试执行次数和执行间隔
             */
            ScheduleSnapshot.Topology.Node node = scheduleSnapshot.analyzeCurrentNode(nodeExecuteHistory.getScheduleTopologyNodeId());
            if (node.retries() == 0){
                return;
            }
            if (nodeExecuteHistory.getScheduleRetryNum() != null && nodeExecuteHistory.getScheduleRetryNum() >= node.retries()){
                return;
            }
            /**
             * 生成重试的执行时间
             */
            Date startAt = DateUtils.addMinutes(new Date(),node.intervals());
            // 构造节点执行历史
            NodeExecuteHistory retryNodeExecuteHistory = NodeExecuteHistory.builder()
                    .scheduleId(nodeExecuteHistory.getScheduleId())
                    .scheduleTopologyNodeId(nodeExecuteHistory.getScheduleTopologyNodeId())
                    .scheduleSnapshotId(nodeExecuteHistory.getScheduleSnapshotId())
                    .scheduleInstanceId(nodeExecuteHistory.getScheduleInstanceId())
                    .scheduleRetryNum(nodeExecuteHistory.getScheduleRetryNum()!=null? nodeExecuteHistory.getScheduleRetryNum() + 1 : 1)
                    .scheduleHistoryMode(SystemConstants.HistoryMode.RETRY)
                    .scheduleHistoryTime(startAt)
                    .nodeId(nodeExecuteHistory.getNodeId())
                    .nodeType(nodeExecuteHistory.getNodeType())
                    .clusterId(nodeExecuteHistory.getClusterId())
                    .timeout(nodeExecuteHistory.getTimeout())
                    .content(nodeExecuteHistory.getContent())
                    .createTime(nodeExecuteHistory.getCreateTime())
                    .createBy(nodeExecuteHistory.getCreateBy())
                    .build();
            retryNodeExecuteHistory.updateState(SystemConstants.JobState.UN_CONFIRMED_);
            retryNodeExecuteHistory.updateState(SystemConstants.JobState.WAITING_PARENT_);
            retryNodeExecuteHistory.updateState(SystemConstants.JobState.INITED);
            nodeExecuteHistoryService.saveOrUpdate(retryNodeExecuteHistory);
            retryNodeExecuteHistory = nodeExecuteHistoryService.getById(retryNodeExecuteHistory.getId());

            // todo 节点执行Job
            /**
             *
             *
             *
             *
             */

        }

    }


}
