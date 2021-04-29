package com.flink.platform.web.listener;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.*;
import com.flink.platform.web.job.MonitorJob;
import com.flink.platform.web.job.NodeExecuteHistoryYarnStateRefreshJob;
import com.flink.platform.web.job.ScheduleJob;
import com.flink.platform.web.job.ScheduleSubmitJob;
import com.flink.platform.web.job.system.ActiveYarnAppRefreshJob;
import com.flink.platform.web.service.*;
import com.flink.platform.web.utils.SchedulerUtils;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 当上下文已经准备完毕的时候触发
 * <p>
 * 1. 启动常驻任务
 * 2. 启动任务调度
 * 3. 启动监控
 *
 * <p>
 * Created by 凌战 on 2021/4/15
 */
@Component
public class ApplicationReadyListener implements ApplicationListener<ApplicationReadyEvent> {

    private final Logger logger = LoggerFactory.getLogger(ApplicationReadyListener.class);
    private final DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    @Autowired
    private MonitorService monitorService;
    @Autowired
    private ScheduleService scheduleService;
    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;
    @Autowired
    private ScheduleSnapshotService scheduleSnapshotService;
    @Autowired
    private ScheduleNodeService scheduleNodeService;


    @Override
    public void onApplicationEvent(ApplicationReadyEvent applicationReadyEvent) {
        logger.warn("Starting necessary task");
        // 启动常驻任务
        startResidentMission();
        // 启动任务调度
        startSchedule();
        // 启动监控
        startMonitor();
        try {
            SchedulerUtils.getScheduler().start();
        } catch (SchedulerException e) {
            logger.error(e.getMessage(), e);
        }

    }


    /**
     * 启动常驻的任务:
     * 1. yarn应用列表状态更新(长时间未运行;占用内存过大;重复应用提交)
     * 2. 作业状态更新任务()
     */
    private void startResidentMission() {
        /**
         * 启动任务:yarn应用列表状态更新
         */
        SchedulerUtils.scheduleCronJob(ActiveYarnAppRefreshJob.class, "*/10 * * * * ?");
        /**
         * 启动作业状态更新任务:
         * 主要是将保存到数据库中的各节点执行实例状态与yarn上的app状态进行对比
         * 1.仍然还在yarn上运行中的任务,就更新数据库中的节点执行实例,状态为运行中
         * 2.已经不在yarn上运行的任务,去获取最近完成的任务,将相应的一些属性赋值进去
         */
        SchedulerUtils.scheduleCronJob(NodeExecuteHistoryYarnStateRefreshJob.class, "*/5 * * * * ?");

        SchedulerUtils.scheduleCronJob(ScheduleSubmitJob.class,"*/1 * * * * ?");




    }


    /**
     * 启动任务调度:
     * 1.对那些依然开启的任务
     * 2.找出对应的执行节点
     * 3.挨个生成执行历史
     */
    private void startSchedule() {
        // 找出所有enable为true的定时调度任务
        List<Schedule> schedules = scheduleService.list(new QueryWrapper<Schedule>().eq("enable", true));
        Date now = new Date();
        // 更新时间
        schedules.forEach(schedule -> {
            // max(调度设置的开始时间,当前时间)
            Date nextFireTime = ScheduleJob.getNextFireTime(schedule.generateCron(), schedule.getStartTime().compareTo(now) <= 0 ? now : schedule.getStartTime());
            // 调度实例id
            String scheduleInstanceId = dateFormat.format(nextFireTime);
            // 查找当前调度的还未执行的节点执行历史
            List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                    .eq("schedule_id", schedule.getId())
                    .eq("state", SystemConstants.JobState.UN_CONFIRMED_)
            );
            ScheduleSnapshot scheduleSnapshot = scheduleSnapshotService.findByScheduleIdAndSnapshotTime(schedule.getId(), now);
            /**
             * 对节点执行实例查漏补缺
             */
            dealHistory(null, scheduleInstanceId, scheduleSnapshot, 0, nodeExecuteHistories);
            ScheduleJob.build(schedule);
        });

    }


    /**
     * 启动监控:一般是流处理任务监控
     * 每一个Monitor实例的id,都会绑定在一个ScheduleNode上
     * (所以监控的粒度是基于节点级别的)
     */
    private void startMonitor() {
        List<Monitor> monitors = monitorService.list(new QueryWrapper<Monitor>()
                .eq("enabled", true));
        monitors.forEach(MonitorJob::build);
    }

    /**
     * 递归生成每个节点的执行实例
     *
     * @param scheduleTopologyNodeId 拓扑节点
     * @param scheduleInstanceId     调度实例id
     * @param scheduleSnapshot       最近的调度快照
     * @param generateStatus         节点产生状态
     * @param nodeExecuteHistories   节点执行历史
     */
    private void dealHistory(String scheduleTopologyNodeId,
                             String scheduleInstanceId,
                             ScheduleSnapshot scheduleSnapshot,
                             int generateStatus,
                             List<NodeExecuteHistory> nodeExecuteHistories) {

        Map<String, ScheduleSnapshot.Topology.Node> nextNodeIdToObj = scheduleSnapshot.analyzeNextNode(scheduleTopologyNodeId);
        // key: 后节点的id value:
        for (String nodeId : nextNodeIdToObj.keySet()) {
            // 当前节点存在当前调度实例的执行实例
            boolean exist = nodeExecuteHistories.removeIf(nodeExecuteHistory ->
                    nodeExecuteHistory.getScheduleTopologyNodeId().equals(nodeId) &&
                            nodeExecuteHistory.getScheduleInstanceId().equals(scheduleInstanceId)
            );
            if (!exist) {
                ScheduleNode scheduleNode = scheduleNodeService.getOne(
                        new QueryWrapper<ScheduleNode>()
                                .eq("schedule_id", scheduleSnapshot.getScheduleId())
                                .eq("schedule_top_node_id", nodeId)
                );
                scheduleNodeService.generateHistory(scheduleNode, scheduleSnapshot, scheduleInstanceId, generateStatus);
            }

            dealHistory(nodeId, scheduleInstanceId, scheduleSnapshot, generateStatus, nodeExecuteHistories);
        }
    }


}
