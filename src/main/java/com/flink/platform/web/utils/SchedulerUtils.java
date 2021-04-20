package com.flink.platform.web.utils;

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
        ApplicationContext applicationContext= SpringContextUtils.getApplicationContext();
        /**
         * 这里获取的Scheduler实例,是引入spring-boot-starter-quartz后
         * SchedulerFactoryBean中初始化的
         */
        if (applicationContext!=null) {
            scheduler = applicationContext.getBean(Scheduler.class);
        }else{
            // 使用工厂构造一个新的Scheduler
            SchedulerFactory schedulerFactory = new StdSchedulerFactory();
            try{
                scheduler = schedulerFactory.getScheduler();
                scheduler.start();
            }catch (SchedulerException e){
                throw new RuntimeException();
            }
        }
    }

    private SchedulerUtils(){

    }


    public static Scheduler getScheduler() {
        return scheduler;
    }

    public static void scheduleCronJob(Class<? extends Job> jobClass, Object name, String group, String cronExpression, JobDataMap jobDataMap, Date startDate,Date endDate){

    }




}
