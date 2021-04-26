package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.Cluster;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.MonitorService;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.utils.SchedulerUtils;
import com.flink.platform.web.utils.YarnApiUtils;
import lombok.extern.slf4j.Slf4j;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * 监控运行任务
 * <p>
 * Created by 凌战 on 2021/4/25
 */
@Slf4j
public class MonitorJob extends AbstractNoticeableJob implements InterruptableJob {

    private Thread thread;
    private volatile boolean interrupted = false;
    private JobKey jobKey;

    private Monitor monitor;
    private ScheduleNode scheduleNode; // 调度任务执行节点
    private NodeExecuteHistory nodeExecuteHistory;
    private Cluster cluster;


    @Autowired
    private MonitorService monitorService;
    @Autowired
    private ScheduleNodeService scheduleNodeService;
    @Autowired
    private ClusterService clusterService;
    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;


    @Override
    public void interrupt() {
        if (!interrupted) {
            log.info("调度器正在停止这个任务,key:" + this.jobKey.getName());
            interrupted = true;
            thread.interrupt();
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        this.jobKey = jobExecutionContext.getJobDetail().getKey();
        thread = Thread.currentThread();
        Integer monitorId = Integer.parseInt(jobExecutionContext.getJobDetail().getKey().getName());
        // 根据当前定时监控任务的key,找到数据库对应保存的类
        monitor = monitorService.getById(monitorId);
        // 更新monitor实际的触发时间
        monitor.setRealFireTime(jobExecutionContext.getFireTime());
        monitorService.saveOrUpdate(monitor);

        /**
         * 监控任务中执行节点
         */
        scheduleNode = scheduleNodeService.getOne(
                new QueryWrapper<ScheduleNode>()
                        .eq("monitor_id", monitorId)
        );
        nodeExecuteHistory = nodeExecuteHistoryService.
                findNoScheduleLatestByNodeId(scheduleNode.getId());

        /**
         * 没有执行历史
         */
        if (nodeExecuteHistory == null) {
            restart();
            return;
        }
    }

    /**
     * 监控流处理
     */
    private void monitorFlinkStream() {
        // 节点执行历史:在执行中
        if (nodeExecuteHistory.isRunning()) {
            boolean exist = YarnApiUtils.existRunningJobs(cluster.getYarnUrl(), nodeExecuteHistory.getJobId());
            if (!exist) {
                // 5mins还没有运行,重启
                if (System.currentTimeMillis() - nodeExecuteHistory.getStartTime().getTime() >= 300000) {
                    YarnApiUtils.killApp(cluster.getYarnUrl(), nodeExecuteHistory.getJobId());
                    // 节点执行状态更新为KILLED
                    nodeExecuteHistory.updateState(SystemConstants.JobState.KILLED);
                    nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
                    // 重启任务
                    // todo Flink重启任务可以从checkpoint恢复
                    boolean restart = restart();
                    if (restart) {
                        notice(nodeExecuteHistory, SystemConstants.ErrorType.FLINK_STREAM_NO_RUNNING_JOB_RESTART);
                    } else {
                        notice(nodeExecuteHistory, SystemConstants.ErrorType.FLINK_STREAM_NO_RUNNING_JOB_RESTART_FAILED);
                    }
                } else {
                    notice(nodeExecuteHistory, SystemConstants.ErrorType.FLINK_STREAM_NO_RUNNING_JOB);
                }
            } else {
                // 任务阻塞判断
                if (monitor.getWaitingBatches() == 0){
                    return;
                }
                // 获取背压数据




            }

        }
    }


    /**
     * 对于没有执行历史的监控任务节点,重新执行
     */
    private boolean restart() {
        NodeExecuteHistory nodeExecuteHistory = nodeExecuteHistoryService.
                findNoScheduleLatestByNodeId(scheduleNode.getId());
        if (nodeExecuteHistory != null && nodeExecuteHistory.isRunning()) {
            return true;
        }
        return scheduleNodeService.execute(scheduleNode, monitor);
    }


    public static void build(Monitor monitor) {
        SchedulerUtils.scheduleCronJob(MonitorJob.class,
                monitor.getId(),
                SystemConstants.JobGroup.MONITOR,
                monitor.generateCron());
    }


}
