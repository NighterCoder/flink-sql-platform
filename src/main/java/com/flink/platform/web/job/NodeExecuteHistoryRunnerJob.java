package com.flink.platform.web.job;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.service.NodeExecuteHistoryService;
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
        if (nodeExecuteHistoryService.execTimeout(nodeExecuteHistory)){
            return;
        }
        nodeExecuteHistory.updateState(SystemConstants.JobState.SUBMITTING);
        nodeExecuteHistory.setStartTime(new Date());
        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);




    }


}
