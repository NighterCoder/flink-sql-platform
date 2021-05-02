package com.flink.platform.web.job.system;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.Cluster;
import com.flink.platform.web.common.entity.entity2table.ClusterUser;
import com.flink.platform.web.common.entity.HttpYarnApp;
import com.flink.platform.web.common.entity.entity2table.YarnApp;
import com.flink.platform.web.config.YarnConfig;
import com.flink.platform.web.service.ClusterService;
import com.flink.platform.web.service.ClusterUserService;
import com.flink.platform.web.service.YarnAppService;
import com.flink.platform.web.utils.YarnApiUtils;
import org.apache.commons.collections.CollectionUtils;
import org.quartz.InterruptableJob;
import org.quartz.JobExecutionContext;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Yarn上活跃任务应用列表更新任务
 * 1, 重复应用检测
 * 2, 长时间未运行检测
 * 3, 内存超限检测
 * ...
 * <p>
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
        for (Cluster cluster : clusters) {
            Integer clusterId = cluster.getId();
            /**
             * 获取当前集群活跃的Applications
             */
            List<HttpYarnApp> httpYarnApps = YarnApiUtils.getActiveApps(cluster.getYarnUrl());
            // 请求访问出错
            if (httpYarnApps == null) {
                continue;
            }
            // 当前已经没有活跃的applications
            if (httpYarnApps.isEmpty()) {
                // todo 写到Service层
                yarnAppService.remove(
                        new QueryWrapper<YarnApp>().eq("cluster_id", clusterId)
                );
                continue;
            }
            // 将HttpYarnApp转换成YarnApp保存到数据库中
            List<YarnApp> yarnApps = new ArrayList<>();
            for (HttpYarnApp httpYarnApp : httpYarnApps) {
                YarnApp yarnApp = new YarnApp();
                BeanUtils.copyProperties(httpYarnApp, yarnApp);
                // 保存到数据库中生成id
                yarnApp.setId(null);
                yarnApp.setClusterId(clusterId);
                String queue = httpYarnApp.getQueue();
                // 以队列为准
                List<ClusterUser> clusterUsers = clusterUserService.list(new QueryWrapper<ClusterUser>()
                        .eq("cluster_id", clusterId)
                        .eq("queue", queue)
                );
                /**
                 * 默认Yarn上的queue都是以root作为父队列
                 */
                if (clusterUsers.isEmpty()) {
                    if (queue.startsWith("root.")) {
                        // 获取真正的队列名称
                        queue = queue.substring(5);
                        clusterUsers = clusterUserService.list(new QueryWrapper<ClusterUser>()
                                .eq("cluster_id", clusterId)
                                .eq("queue", queue)
                        );
                    }
                }
                /**
                 * 遍历集群当前队列用户
                 */
                if (!clusterUsers.isEmpty()) {
                    boolean match = false;
                    for (ClusterUser clusterUser : clusterUsers) {
                        // 获取User的Id
                        if (yarnApp.getUser().equals(clusterUser.getUser())) {
                            yarnApp.setUserId(clusterUser.getUserId());
                            match = true;
                            break;
                        }
                    }
                    // 没有匹配的用户
                    if (!match) {
                        // todo ？？？？
                        yarnApp.setUserId(clusterUsers.get(0).getUserId());
                    }
                }
                yarnApp.setAppId(httpYarnApp.getId());
                yarnApp.setStartedTime(new Date(httpYarnApp.getStartedTime()));
                yarnApp.setRefreshTime(new Date());
                yarnApps.add(yarnApp);
            }

            // 获取数据库中当前集群上一次保存的YarnApp
            List<YarnApp> oldYarnApps = yarnAppService.list(
                    new QueryWrapper<YarnApp>().eq("cluster_id", clusterId)
            );
            if (yarnApps.size() > 0) {
                /**
                 * id都为null,可以直接保存
                 */
                yarnAppService.saveBatch(yarnApps);
            }
            if (!CollectionUtils.isEmpty(oldYarnApps)) {
                // 获取上一次该集群所有的yarn apps
                List<Integer> ids = new ArrayList<>(oldYarnApps.size());
                oldYarnApps.forEach(yarnApp -> ids.add(yarnApp.getId()));
                yarnAppService.removeByIds(ids);
            }
            /**
             * 30个定时周期检查:
             * 1.重复应用
             * 2.长时间未运行应用
             */
            if (checkAppDuplicateAndNoRunningSkipCount >= 30) {
                checkAppNotRunning(cluster,yarnApps);
                // todo
            }
            if (checkAppMemorySkipCount >= 180) {
                checkAppMemory(cluster,yarnApps);
            }
        }

        if (checkAppDuplicateAndNoRunningSkipCount >= 30) {
            checkAppDuplicateAndNoRunningSkipCount = 0;
        }
        if (checkAppMemorySkipCount >= 180) {
            checkAppMemorySkipCount = 0;
        }
    }


    /**
     * 长时间未运行,即一直处于Accepted状态的任务
     * 10mins未运行
     *
     * @param cluster
     * @param yarnApps
     */
    private void checkAppNotRunning(Cluster cluster, List<YarnApp> yarnApps) {
        long now = System.currentTimeMillis();
        for (YarnApp yarnApp : yarnApps) {
            if (SystemConstants.JobState.ACCEPTED.equals(yarnApp.getState()) &&
                    now - yarnApp.getStartedTime().getTime() >= 600000
            ) {
                String trackingUrl = yarnApp.getTrackingUrl();
                // todo 告警
                // notice(cluster, yarnApp, trackingUrl, Constant.ErrorType.APP_NO_RUNNING);
            }
        }
    }

    /**
     * 大内存应用检查
     *
     * @param cluster
     * @param yarnApps
     */
    private void checkAppMemory(Cluster cluster,List<YarnApp> yarnApps){
        /**
         * 提交应用内存无限制
         */
        if (yarnConfig.getAppMemoryThreshold() <= 0){
            return;
        }
        for (YarnApp yarnApp:yarnApps){
            if ("RUNNING".equalsIgnoreCase(yarnApp.getState()) && yarnApp.getAllocatedMb()!=null){
                if (yarnApp.getAllocatedMb() >= yarnConfig.getAppMemoryThreshold() &&
                        !yarnConfig.getAppWhiteList().contains(yarnApp.getName().split("\\.deer_instance_")[0])) {
                    String trackingUrl = yarnApp.getTrackingUrl();
                    //notice(cluster, yarnApp, trackingUrl, Constant.ErrorType.APP_MEMORY_OVERLIMIT);
                }
            }
        }




    }





}
