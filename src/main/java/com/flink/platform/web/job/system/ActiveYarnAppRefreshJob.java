package com.flink.platform.web.job.system;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.entity.Cluster;
import com.flink.platform.web.common.entity.HttpYarnApp;
import com.flink.platform.web.common.entity.YarnApp;
import com.flink.platform.web.config.YarnConfig;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.ClusterUserService;
import com.flink.platform.web.service.YarnAppService;
import com.flink.platform.web.utils.YarnApiUtils;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * Yarn上活跃任务应用列表更新任务
 * 1, 重复应用检测
 * 2, 长时间未运行检测
 * 3, 内存超限检测
 * ...
 *
 *
 * 保存在yarn_app表(仅保存当前活跃状态)中
 */
public class ActiveYarnAppRefreshJob implements InterruptableJob {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private YarnAppService yarnAppService;

    @Autowired
    private YarnConfig yarnConfig;

    @Autowired
    private ClusterUserService clusterUserService;


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
        if (!interrupted) {
            interrupted = true;
            thread.interrupt();
        }
    }

    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        thread = Thread.currentThread();
        checkAppDuplicateAndNoRunningSkipCount++;
        checkAppMemorySkipCount++;

        // 1.先去遍历所有集群
        List<Cluster> clusters = clusterService.list(null);
        for (Cluster cluster:clusters){
            Integer clusterId = cluster.getId();
            /**
             * 获取当前集群活跃的Applications
             */
            List<HttpYarnApp> yarnApps = YarnApiUtils.getActiveApps(cluster.getYarnUrl());
            // 请求访问出错
            if (yarnApps == null){
                continue;
            }
            // 当前已经没有活跃的applications
            if (yarnApps.isEmpty()){
                // todo 写到Service层
                yarnAppService.remove(
                        new QueryWrapper<YarnApp>().eq("cluster_id",clusterId)
                );
                continue;
            }
            // 将HttpYarnApp转换成YarnApp保存到数据库中








        }

    }
}
