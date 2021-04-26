package com.flink.platform.web.listener;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.job.MonitorJob;
import com.flink.platform.web.job.system.ActiveYarnAppRefreshJob;
import com.flink.platform.web.service.MonitorService;
import com.flink.platform.web.utils.SchedulerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 当上下文已经准备完毕的时候触发
 * <p>
 * Created by 凌战 on 2021/4/15
 */
@Component
public class ApplicationReadyListener implements ApplicationListener<ApplicationReadyEvent> {

    private final Logger logger = LoggerFactory.getLogger(ApplicationReadyListener.class);

    @Autowired
    private MonitorService monitorService;

    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        logger.warn("Starting necessary task");
        // 启动常驻任务
        startResidentMission();
        // todo 启动任务调度


        startMonitor();
    }


    /**
     * 启动常驻的任务:
     * 1. yarn应用列表状态更新(长时间未运行;占用内存过大;重复应用提交)
     * 2. 作业状态更新任务()
     */
    private void startResidentMission() {
        // 启动任务:yarn应用列表状态更新
        SchedulerUtils.scheduleCronJob(ActiveYarnAppRefreshJob.class, "*/10 * * * * ?");
    }


    /**
     * 启动监控:
     * 每一个Monitor实例的id,都会绑定在一个ScheduleNode上
     * (所以监控的粒度是基于节点级别的)
     */
    private void startMonitor() {
        List<Monitor> monitors = monitorService.list(new QueryWrapper<Monitor>()
                .eq("enabled", true));
        monitors.forEach(MonitorJob::build);
    }


}
