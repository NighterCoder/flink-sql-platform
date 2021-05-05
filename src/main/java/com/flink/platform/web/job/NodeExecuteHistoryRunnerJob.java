package com.flink.platform.web.job;

import com.flink.platform.core.config.entries.ExecutionEntry;
import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.impl.FlinkJobService;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;


/**
 * todo 节点实例执行任务
 * <p> 不同的节点类型提交执行任务是不一样的
 * <p>
 * <p>
 * Created by 凌战 on 2021/4/28
 */
@DisallowConcurrentExecution
public class NodeExecuteHistoryRunnerJob extends AbstractRetryableJob implements InterruptableJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(NodeExecuteHistoryRunnerJob.class);

    private Thread thread;
    private volatile boolean interrupted = false;
    private NodeExecuteHistory nodeExecuteHistory;

    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private FlinkJobService flinkJobService;

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
        Integer nodeExecuteHistoryId = Integer.parseInt(jobExecutionContext.getJobDetail().getKey().getName());
        nodeExecuteHistory = nodeExecuteHistoryService.getById(nodeExecuteHistoryId);
        /**
         * todo ??? 在这里判断超时合适吗
         */
        if (nodeExecuteHistoryService.execTimeout(nodeExecuteHistory)) {
            return;
        }
        nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTING);
        nodeExecuteHistory.setStartTime(new Date());
        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);

        /**
         * 当前执行节点的json串
         */
        String content = nodeExecuteHistory.getContent();
        String nodeType = nodeExecuteHistory.getNodeType();

        try {
            if (SystemConstants.NodeType.FLINK_BATCH_SQL.equals(nodeType) ||
                    SystemConstants.NodeType.FLINK_STREAM_SQL.equals(nodeType)) {
                // 提交Flink SQL任务
                runFlinkSQL(content, nodeType, nodeExecuteHistory);
            } else if (SystemConstants.NodeType.FLINK_STREAM_JAR.equals(nodeType)) {
                // todo 提交Flink Jar任务
            } else if (SystemConstants.NodeType.SPARK_BATCH_SQL.equals(nodeType)) {
                // todo 提交Spark SQL任务
            } else if (SystemConstants.NodeType.SPARK_BATCH_JAR.equals(nodeType)) {
                // todo 提交Spark Jar任务
            } else {
                throw new RuntimeException("不支持当前节点类型:" + nodeType + ",请联系开发人员");
            }
        } catch (Exception e) {
            if (interrupted) {
                dealInterrupted();
                return;
            }
            LOGGER.error(e.getMessage(), e);
            if (nodeExecuteHistory.getSteps().contains(SystemConstants.JobState.SUBMITTED)) {
                nodeExecuteHistory.updateState(SystemConstants.JobState.FAILED);
            } else {
                nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTING_FAILED);
            }
            nodeExecuteHistory.setFinishTime(new Date());
            nodeExecuteHistory.setErrors(e.getMessage());
            nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
            // 重试机制,本身yarn有重试机制,但是这里还是选择调用重试方法
            retryCurrentNode(nodeExecuteHistory, SystemConstants.JobState.FAILED);
        }
    }

    /**
     * 提交执行Flink SQL任务
     * 1.创建获取Session
     * 2.提交执行Flink SQL
     * 3.获取结果
     *
     * @param content 这里的content直接就是SQL
     */
    private void runFlinkSQL(String content, String nodeType, NodeExecuteHistory nodeExecuteHistory) {
        String sessionId;
        FlinkSessionCreateParam param = new FlinkSessionCreateParam();
        param.setSessionName(nodeExecuteHistory.getScheduleId() + "_" + nodeExecuteHistory.getScheduleInstanceId() + "_" + nodeExecuteHistory.getNodeId());
        if (SystemConstants.NodeType.FLINK_STREAM_SQL.equals(nodeType)) {
            param.setExecutionType(ExecutionEntry.EXECUTION_TYPE_VALUE_STREAMING);
        } else {
            param.setExecutionType(ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH);
        }

        sessionId = flinkJobService.createSession(param);
        StatementResult result = flinkJobService.submit(content, sessionId);
        if (!interrupted) {
            if (result != null) {
                nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTED);
                nodeExecuteHistory.setJobId(result.getJobId());
                String yarnUrl = clusterService.getById(nodeExecuteHistory.getClusterId()).getYarnUrl();
                nodeExecuteHistory.setJobUrl(yarnUrl + "/proxy/" + result.getJobId() + "/");
                nodeExecuteHistory.setJobFinalStatus("UNDEFINED");
            } else {
                nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTING_FAILED);
                nodeExecuteHistory.setFinishTime(new Date());
            }
            nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
        }else{
            dealInterrupted();
        }
    }


    /**
     * 处理 interrupt 方法
     */
    private void dealInterrupted() {

    }


}
