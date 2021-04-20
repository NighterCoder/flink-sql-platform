package com.flink.platform.web.job.system;

import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

/**
 * Yarn上活跃任务应用列表更新任务
 * 1, 重复应用检测
 * 2, 长时间未运行检测
 * 3, 内存超限检测
 * ...
 *
 */
public class ActiveYarnAppRefreshJob implements InterruptableJob {


    private static int checkAppDuplicateAndNoRunningSkipCount = 0;
    private static int checkAppMemorySkipCount = 0;
    /**
     * 使用volatile修饰
     */
    private volatile boolean interrupted = false;
    private Thread thread;

    /**
     * 任务被中断时调用
     */
    @Override
    public void interrupt() {
        /**
         * interrupted为false,则停止任务
         */
        if(!interrupted){
            interrupted = true;
            thread.interrupt();
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

    }
}
