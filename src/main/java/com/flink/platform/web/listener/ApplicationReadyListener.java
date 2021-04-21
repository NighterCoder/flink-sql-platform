package com.flink.platform.web.listener;

import com.flink.platform.web.job.system.ActiveYarnAppRefreshJob;
import com.flink.platform.web.utils.SchedulerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

/**
 * 当上下文已经准备完毕的时候触发
 * <p>
 * Created by 凌战 on 2021/4/15
 */
@Component
public class ApplicationReadyListener implements ApplicationListener<ApplicationReadyEvent> {

    private final Logger logger = LoggerFactory.getLogger(ApplicationReadyListener.class);


    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        logger.warn("Starting necessary task");
        // 启动常驻任务
        startResidentMission();


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


}
