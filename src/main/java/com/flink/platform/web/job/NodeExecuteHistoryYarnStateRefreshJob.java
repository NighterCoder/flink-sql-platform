package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.HttpYarnApp;
import com.flink.platform.web.common.entity.entity2table.Cluster;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.utils.YarnApiUtils;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 调度节点执行实例状态更新任务
 * <p>
 * Created by 凌战 on 2021/4/28
 */
@DisallowConcurrentExecution
public class NodeExecuteHistoryYarnStateRefreshJob extends AbstractRetryableJob implements InterruptableJob {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss");

    private Thread thread;
    private volatile boolean interrupted = false;


    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ScheduleNodeService scheduleNodeService;


    @Override
    public void interrupt() {
        if (!interrupted) {
            interrupted = true;
            thread.interrupt();
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        thread = Thread.currentThread();
        // 遍历集群
        List<Cluster> clusters = clusterService.list(null);
        for (Cluster cluster : clusters) {
            // 找出当前集群的所有执行节点
            List<ScheduleNode> scheduleNodes = scheduleNodeService.list(
                    new QueryWrapper<ScheduleNode>()
                            .eq("cluster_id", cluster.getId())
            );
            // 当前集群没有执行节点
            if (scheduleNodes.isEmpty()) {
                continue;
            }
            List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                    .eq("cluster_id", cluster.getId())
                    .eq("job_final_status", "UNDEFINED")
            );
            if (nodeExecuteHistories.isEmpty()) {
                continue;
            }
            // 获取当前正在活跃的Application
            List<HttpYarnApp> httpYarnApps = YarnApiUtils.getActiveApps(cluster.getYarnUrl());
            if (httpYarnApps == null) {
                continue;
            }
            // 排除不是通过平台提交的任务
            httpYarnApps.removeIf(httpYarnApp -> !httpYarnApp.getName().contains(".deer_instance_") && !httpYarnApp.getName().contains(".deer_test_instance_"));

            Map<Integer, ScheduleNode> nodeId2ObjMap = new HashMap<>();
            scheduleNodes.forEach(scheduleNode -> nodeId2ObjMap.put(scheduleNode.getId(), scheduleNode));
            Map<String, NodeExecuteHistory> nodeUserAndQueueAndName2NodeHistoryMap = new HashMap<>();
            nodeExecuteHistories.forEach(nodeExecuteHistory -> {
                ScheduleNode scheduleNode = nodeId2ObjMap.get(nodeExecuteHistory.getNodeId());
                if (scheduleNode != null) {
                    String user = scheduleNode.getUser();
                    String queue = scheduleNode.getQueue();
                    if (queue != null && !"root".equals(queue) && !queue.startsWith("root.")) {
                        queue = "root." + queue;
                    }
                    String key = user + "$" + queue + "$" + scheduleNode.getApp() + ".deer_instance_" + (scheduleNode.isBatch() ? "b" : "s")
                            + DATE_FORMAT.format(nodeExecuteHistory.getCreateTime());
                    nodeUserAndQueueAndName2NodeHistoryMap.put(key, nodeExecuteHistory);

                }
            });

            Set<Integer> matchIds = new HashSet<>();
            // 活跃的App与数据库中保存的进行匹配
            for (HttpYarnApp httpYarnApp : httpYarnApps) {
                String key = httpYarnApp.getUser() + "$" + httpYarnApp.getQueue() + "$" + httpYarnApp.getName();
                // 数据库中不存在当前key
                if (!nodeUserAndQueueAndName2NodeHistoryMap.containsKey(key)) {
                    // 测试任务,直接删除
                    if (key.contains(".deer_test_instance_")) {
                        YarnApiUtils.killApp(cluster.getYarnUrl(), httpYarnApp.getId());
                    }
                    continue;
                }
                NodeExecuteHistory nodeExecuteHistory = nodeUserAndQueueAndName2NodeHistoryMap.get(key);
                updateMatchNodeExecuteHistory(httpYarnApp, nodeExecuteHistory);
                matchIds.add(nodeExecuteHistory.getId());
            }

            /**
             * 再遍历一次,找出不匹配的那些任务
             */
            nodeExecuteHistories.forEach(nodeExecuteHistory -> {
                if (!matchIds.contains(nodeExecuteHistory.getId())) {
                    /**
                     * todo ??? 为什么是 getContent 方法
                     */
                    if (nodeExecuteHistory.getContent().contains(".deer_test_instance_")) {
                        nodeExecuteHistory.updateState(SystemConstants.JobState.KILLED);
                        nodeExecuteHistory.setJobFinalStatus(SystemConstants.JobState.KILLED);
                        nodeExecuteHistory.setFinishTime(new Date());
                        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
                    } else {
                        updateNoMatchNodeExecuteHistory(cluster.getYarnUrl(), nodeExecuteHistory, nodeId2ObjMap);
                    }
                }
            });
        }
    }


    /**
     * 仍然在yarn上还处于运行状态中的任务
     * 将在yarn上实际运行任务的状态,更新到保存到数据库中的实例nodeExecuteHistory
     *
     * @param httpYarnApp        yarn上的应用
     * @param nodeExecuteHistory 节点执行历史
     */
    private void updateMatchNodeExecuteHistory(HttpYarnApp httpYarnApp, NodeExecuteHistory nodeExecuteHistory) {
        if ("RUNNING".equals(httpYarnApp.getState()) ||
                "FINAL_SAVING".equals(httpYarnApp.getState()) ||
                "KILLING".equals(httpYarnApp.getState()) ||
                "FINISHING".equals(httpYarnApp.getState())) {
            nodeExecuteHistory.updateState(SystemConstants.JobState.ACCEPTED);
            nodeExecuteHistory.updateState(SystemConstants.JobState.RUNNING);
        } else {
            nodeExecuteHistory.updateState(httpYarnApp.getState());
        }
        /**
         * 第一次更新状态: yarn的ApplicationId,对应的ApplicationUrl,对应的任务最终状态
         */
        nodeExecuteHistory.setJobId(httpYarnApp.getId());
        nodeExecuteHistory.setJobUrl(httpYarnApp.getTrackingUrl());
        nodeExecuteHistory.setJobFinalStatus(httpYarnApp.getFinalStatus());
        nodeExecuteHistory.setStartTime(new Date(httpYarnApp.getStartedTime()));
        /**
         * 还没有执行完,不需要更新结束时间
         */
        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
    }


    /**
     * 不在yarn上运行的任务,但是数据库中存在(State为FINISHED,KILLED,FAILED; FINAL_STATUS为SUCCEEDED,KILLED,FAILED),
     * 这些任务已经执行完成,或许成功,或许失败
     * <p>
     * 注意:state为FINISHED,对应的final_status可能为SUCCEEDED,KILLED,FAILED ===> 所以更新状态是使用final_status
     * <p>
     * 当前用户,队列,应用名
     *
     * @param yarnUrl            yarn的url
     * @param nodeExecuteHistory 节点执行实例
     * @param nodeId2ObjMap      节点保存
     */
    private void updateNoMatchNodeExecuteHistory(String yarnUrl, NodeExecuteHistory nodeExecuteHistory, Map<Integer, ScheduleNode> nodeId2ObjMap) {
        ScheduleNode scheduleNode = nodeId2ObjMap.get(nodeExecuteHistory.getNodeId());
        HttpYarnApp httpYarnApp = YarnApiUtils.getLastNoActiveApp(yarnUrl, scheduleNode.getUser(), scheduleNode.getQueue(),
                scheduleNode.getApp() + ".deer_instance_" + (scheduleNode.isBatch() ? "b" : "s") + DATE_FORMAT.format(scheduleNode.getCreateTime()), 3);
        if (httpYarnApp != null) {
            if ("FINISHED".equals(httpYarnApp.getState())) {
                nodeExecuteHistory.updateState(httpYarnApp.getFinalStatus());
            } else {
                nodeExecuteHistory.updateState(httpYarnApp.getState());
            }

            nodeExecuteHistory.setJobId(httpYarnApp.getId());
            nodeExecuteHistory.setJobUrl(httpYarnApp.getTrackingUrl());
            nodeExecuteHistory.setJobFinalStatus(httpYarnApp.getFinalStatus());

            /**
             * 运行失败的任务需要记录错误日志
             */
            if ("FAILED".equals(httpYarnApp.getFinalStatus())) {
                // 导出错误日志
                if (httpYarnApp.getDiagnostics() != null) {
                    if (httpYarnApp.getDiagnostics().length() > 61440) {
                        nodeExecuteHistory.setErrors(httpYarnApp.getDiagnostics().substring(0, 61440));
                    } else {
                        nodeExecuteHistory.setErrors(httpYarnApp.getDiagnostics());
                    }
                }
            }
            nodeExecuteHistory.setStartTime(new Date(httpYarnApp.getStartedTime()));
            nodeExecuteHistory.setFinishTime(new Date(httpYarnApp.getFinishedTime()));
        } else {
            /**
             * 未知状况的节点执行
             */
            nodeExecuteHistory.updateState(SystemConstants.JobState.FAILED);
            nodeExecuteHistory.setJobFinalStatus("UNKNOWN");
            nodeExecuteHistory.setFinishTime(new Date());
        }

        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
        // 对于失败的任务需要重试
        if (!"SUCCEEDED".equals(nodeExecuteHistory.getJobFinalStatus())) {
            if (SystemConstants.NodeType.SPARK_BATCH_JAR.equals(nodeExecuteHistory.getNodeType()) ||
                    SystemConstants.NodeType.SPARK_BATCH_SQL.equals(nodeExecuteHistory.getNodeType())) {
                retryCurrentNode(nodeExecuteHistory, String.format(SystemConstants.ErrorType.SPARK_BATCH_UNUSUAL, nodeExecuteHistory.getJobFinalStatus()));
            } else if (SystemConstants.NodeType.FLINK_BATCH_SQL.equals(nodeExecuteHistory.getNodeType())) {
                retryCurrentNode(nodeExecuteHistory, String.format(SystemConstants.ErrorType.FLINK_BATCH_UNUSUAL, nodeExecuteHistory.getJobFinalStatus()));
            }
        }
    }


}
