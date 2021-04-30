package com.flink.platform.web.job;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.job.system.ActiveYarnAppRefreshJob;
import com.flink.platform.web.service.MonitorService;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.utils.SchedulerUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;
import java.util.List;


/**
 * 平台的超时任务,主要是
 * 监控任务,
 * <p>
 * <p>
 * <p>
 * Created by 凌战 on 2021/4/30
 */
public class PlatformTimeoutJob extends AbstractNoticeableJob implements Job {

    private static final Logger LOGGER = LoggerFactory.getLogger(PlatformTimeoutJob.class);


    @Autowired
    private MonitorService monitorService;
    @Autowired
    private ScheduleNodeService scheduleNodeService;
    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        List<JobExecutionContext> executionContexts;
        try {
            // 获取正在执行的任务
            executionContexts = SchedulerUtils.getScheduler().getCurrentlyExecutingJobs();
        } catch (SchedulerException e) {
            LOGGER.error(e.getMessage(), e);
            return;
        }

        Date current = new Date();
        Date tenMinBefore = DateUtils.addMinutes(current, -10);
        executionContexts.forEach(executionContext -> {
            // 10min中过去了没有结束完成
            if (executionContext.getFireTime().before(tenMinBefore)) {
                JobKey jobKey = executionContext.getJobDetail().getKey();

                if (SystemConstants.JobGroup.COMMON.equals(jobKey.getGroup())) {

                    // Yarn应用更新
                    if (ActiveYarnAppRefreshJob.class.getSimpleName().equals(jobKey.getName())) {
                        notice("调度平台-Yarn应用列表更新任务", "系统任务运行超时");
                        SchedulerUtils.interruptJob(jobKey, SystemConstants.JobGroup.COMMON);
                    }
                    if (NodeExecuteHistoryYarnStateRefreshJob.class.getSimpleName().equals(jobKey.getName())) {
                        notice("调度平台-Yarn应用状态更新任务", "系统任务运行超时");
                        SchedulerUtils.interruptJob(jobKey.getName(), jobKey.getGroup());
                    }
                }

                // 监控任务
                if (SystemConstants.JobGroup.MONITOR.equals(jobKey.getName())) {
                    // 杀掉应用
                    Monitor monitor = monitorService.getById(Integer.parseInt(jobKey.getName()));
                    //scheduleNodeService.getOne()

                    //NodeExecuteHistory nodeExecuteHistory = nodeExecuteHistoryService.findNoScheduleLatestByNodeId()

                }

            }


        });


    }


}
