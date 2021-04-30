package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.HttpYarnApp;
import com.flink.platform.web.common.entity.entity2table.Cluster;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.utils.SchedulerUtils;
import com.flink.platform.web.utils.YarnApiUtils;
import org.apache.commons.collections.CollectionUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Created by 凌战 on 2021/4/29
 */
@DisallowConcurrentExecution
public class NodeExecuteHistoryTimeoutJob extends AbstractRetryableJob implements Job {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

    private static final String[] RUNNING_STATES = new String[]{
            SystemConstants.JobState.INITED,
            SystemConstants.JobState.SUBMITTING,
            SystemConstants.JobState.SUBMITTED,
            SystemConstants.JobState.ACCEPTED,
            SystemConstants.JobState.RUNNING
    };

    @Autowired
    private ClusterService clusterService;
    @Autowired
    private ScheduleNodeService scheduleNodeService;


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        // 查找出处于运行状态,但是没有最终任务执行状态的节点执行实例
        List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .in("state", RUNNING_STATES)
                .isNull("job_final_status")
        );
        if (CollectionUtils.isNotEmpty(nodeExecuteHistories)) {
            return;
        }
        for (NodeExecuteHistory nodeExecuteHistory : nodeExecuteHistories) {
            if (nodeExecuteHistoryService.execTimeout(nodeExecuteHistory)) {
                boolean retry = true;
                // 超时会有很多种情况: 1.一直处于Accepted状态提交不上去;
                // 2.已经提交了,但是执行时间过长
                if (nodeExecuteHistory.getSteps().contains(SystemConstants.JobState.SUBMITTED)) {
                    nodeExecuteHistory.updateState(SystemConstants.JobState.TIMEOUT);
                    nodeExecuteHistory.setFinishTime(new Date());
                } else {
                    // Yarn资源不够,客户端会长时间处于请求状态
                    // ???Accepted状态
                    if (nodeExecuteHistory.getClusterId() != null &&
                            nodeExecuteHistory.getSteps().contains(SystemConstants.JobState.SUBMITTING)) {
                        Cluster cluster = clusterService.getById(nodeExecuteHistory.getClusterId());
                        ScheduleNode scheduleNode = scheduleNodeService.getById(nodeExecuteHistory.getNodeId());

                        // 再去查询一次yarn上app的状态
                        HttpYarnApp httpYarnApp = YarnApiUtils.getActiveApp(cluster.getYarnUrl(), scheduleNode.getUser(), scheduleNode.getQueue(),
                                scheduleNode.getApp() + ".deer_instance_" + (scheduleNode.isBatch() ? "b" : "s") + DATE_FORMAT.format(scheduleNode.getCreateTime()), 3);
                        // 如果没有的话,说明已经提交了
                        if (httpYarnApp != null) {
                            retry = false;
                            nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTED);
                            // 状态未知
                            nodeExecuteHistory.setJobFinalStatus("UNDEFINED");
                        } else {
                            nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTING_TIMEOUT);
                            nodeExecuteHistory.setFinishTime(new Date());
                        }
                    } else {
                        nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTING_TIMEOUT);
                        nodeExecuteHistory.setFinishTime(new Date());
                    }
                }

                nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);

                // 处理调度
                /**
                 * 先暂停任务,再删除任务
                 */
                SchedulerUtils.pauseJob(nodeExecuteHistory.getId(), SystemConstants.JobGroup.SCRIPT_HISTORY);
                SchedulerUtils.deleteJob(nodeExecuteHistory.getId(), SystemConstants.JobGroup.SCRIPT_HISTORY);


                if (retry) {
                    retryCurrentNode(nodeExecuteHistory, SystemConstants.ErrorType.TIMEOUT);
                }

            }
        }


    }


}
