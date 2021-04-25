package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.entity.entity2table.Cluster;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.MonitorService;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleNodeService;
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
                .eq("monitor_id",monitorId)
        );






    }


}