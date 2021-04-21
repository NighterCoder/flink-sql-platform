package com.flink.platform.web.utils;

import com.flink.platform.web.common.SystemConstants;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import java.util.Date;

/**
 * Created by 凌战 on 2021/4/15
 */
public class SchedulerUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchedulerUtils.class);

    private static Scheduler scheduler;

    /**
     * 静态代码先执行?? 还是ApplicationContext先初始化??
     */
    static {
        // 获取ApplicationContext
        ApplicationContext applicationContext = SpringContextUtils.getApplicationContext();
        /**
         * 这里获取的Scheduler实例,是引入spring-boot-starter-quartz后
         * SchedulerFactoryBean中初始化的
         */
        if (applicationContext != null) {
            scheduler = applicationContext.getBean(Scheduler.class);
        } else {
            // 使用工厂构造一个新的Scheduler
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            try {
                scheduler = schedulerFactory.getScheduler();
                scheduler.start();
            } catch (SchedulerException e) {
                throw new RuntimeException();
            }
        }
    }

    private SchedulerUtils() {

    }

    public static Scheduler getScheduler() {
        return scheduler;
    }

    public static void scheduleCronJob(Class<? extends Job> jobClass, String cronExpression) {
        scheduleCronJob(jobClass, jobClass.getSimpleName(), cronExpression);
    }

    public static void scheduleCronJob(Class<? extends Job> jobClass, Object name, String cronExpression) {
        scheduleCronJob(jobClass, name, SystemConstants.JobGroup.COMMON, cronExpression);
    }

    public static void scheduleCronJob(Class<? extends Job> jobClass, Object name, String group, String cronExpression) {
        scheduleCronJob(jobClass, name, group, cronExpression, null);
    }

    public static void scheduleCronJob(Class<? extends Job> jobClass, Object name, String group, String cronExpression, JobDataMap jobDataMap) {
        scheduleCronJob(jobClass, name, group, cronExpression, jobDataMap, null, null);
    }

    public static void scheduleCronJob(Class<? extends Job> jobClass, Object name, String group, String cronExpression, JobDataMap jobDataMap, Date startDate, Date endDate) {
        try {
            // 标识任务
            JobKey jobKey = new JobKey(String.valueOf(name), group);
            // 不存在当前任务
            if (!scheduler.checkExists(jobKey)) {
                JobBuilder jobBuilder = JobBuilder.newJob(jobClass);
                // 绑定标识
                jobBuilder.withIdentity(jobKey);
                /**
                 * jobDataMap设置任务执行配置信息
                 */
                if (jobDataMap != null && !jobDataMap.isEmpty()) {
                    jobBuilder.setJobData(jobDataMap);
                }
                /**
                 * JobDetail是描述Job的类,每次执行会根据JobDetail生成一个Job实例
                 */
                JobDetail jobDetail = jobBuilder.build();
                /**
                 * cron表达式,任务执行周期
                 */
                CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cronExpression);
                TriggerBuilder<CronTrigger> triggerBuilder = TriggerBuilder.newTrigger().withSchedule(cronScheduleBuilder);
                if (startDate != null) {
                    triggerBuilder.startAt(startDate);
                } else {
                    triggerBuilder.startNow();
                }

                if (endDate != null) {
                    triggerBuilder.endAt(endDate);
                }
                CronTrigger trigger = triggerBuilder.build();
                scheduler.scheduleJob(jobDetail, trigger);
            }
        } catch (Exception e) {
            LOGGER.error("Submit job error, name=" + name + " and group=" + group, e);
        }
    }


}
