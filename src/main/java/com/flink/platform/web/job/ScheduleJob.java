package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.service.ScheduleService;
import com.flink.platform.web.service.ScheduleSnapshotService;
import com.flink.platform.web.utils.SchedulerUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.quartz.*;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 更新定时任务相关信息的job
 * <p>
 * Created by 凌战 on 2021/4/23
 */
public class ScheduleJob implements Job {

    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private ScheduleSnapshotService scheduleSnapshotService;

    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;

    @Autowired
    private ScheduleNodeService scheduleNodeService;

    /**
     * 当前Job执行的核心逻辑
     *
     * @param jobExecutionContext 任务执行上下文
     */
    @Override
    public void execute(JobExecutionContext jobExecutionContext) {
        Integer scheduleId = Integer.parseInt(jobExecutionContext.getJobDetail().getKey().getName());
        Schedule schedule = scheduleService.getById(scheduleId);
        // todo 具体时间含义
        schedule.setRealFireTime(jobExecutionContext.getFireTime()); // 真实触发时间
        schedule.setNeedFireTime(jobExecutionContext.getScheduledFireTime()); // 计划触发时间
        schedule.setNextFireTime(jobExecutionContext.getNextFireTime()); // 下次触发时间
        scheduleService.saveOrUpdate(schedule);

        // 根据scheduleId和计划触发时间查找最近的快照
        ScheduleSnapshot scheduleSnapshot = scheduleSnapshotService.findByScheduleIdAndSnapshotTime(scheduleId, jobExecutionContext.getScheduledFireTime());
        if (scheduleSnapshot == null) {
            return;
        }
        confirmedNeed(jobExecutionContext, scheduleSnapshot);
    }


    private void confirmedNeed(JobExecutionContext jobExecutionContext, ScheduleSnapshot scheduleSnapshot) {
        // 调度实例id由计划执行时间转换得来
        String scheduleInstanceId = dateFormat.format(jobExecutionContext.getScheduledFireTime());
        /**
         * 查找当前调度的当前实例的未确定的执行节点
         */
        List<NodeExecuteHistory> executeHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .eq("schedule_id", scheduleSnapshot.getScheduleId())
                .eq("schedule_instance_id", scheduleInstanceId)
                .eq("state", SystemConstants.JobState.UN_CONFIRMED_)
        );
        // 没有计划执行时间的执行历史
        if (executeHistoryList.isEmpty()) {
            // 去查找下次触发时间对应的调度节点历史
            scheduleInstanceId = dateFormat.format(jobExecutionContext.getNextFireTime());
            executeHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                    .eq("schedule_id", scheduleSnapshot.getScheduleId())
                    .eq("schedule_instance_id", scheduleInstanceId)
            );
            // 下次触发时间对应的调度,对应节点执行历史为空
            if (executeHistoryList.isEmpty()) {
                executeHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                        .eq("schedule_id", scheduleSnapshot.getScheduleId())
                        .eq("state", SystemConstants.JobState.UN_CONFIRMED_)
                );
                executeHistoryList.forEach(nodeExecuteHistory -> nodeExecuteHistoryService.missingScheduling(nodeExecuteHistory));
                prepareNext(jobExecutionContext, scheduleSnapshot);
            }
            //
            return;
        }
        generateHistory(null, scheduleInstanceId, scheduleSnapshot, 1);
        prepareNext(jobExecutionContext, scheduleSnapshot);

    }

    /**
     * 生成下次调度执行的实例
     *
     * @param jobExecutionContext
     * @param scheduleSnapshot
     */
    private void prepareNext(JobExecutionContext jobExecutionContext, ScheduleSnapshot scheduleSnapshot) {
        String scheduleInstanceId = dateFormat.format(jobExecutionContext.getNextFireTime());
        generateHistory(null, scheduleInstanceId, scheduleSnapshot, 0);
    }


    /**
     *
     *
     * @param scheduleTopologyNodeId
     * @param scheduleInstanceId
     * @param scheduleSnapshot
     * @param generateStatus
     */
    private void generateHistory(String scheduleTopologyNodeId, String scheduleInstanceId, ScheduleSnapshot scheduleSnapshot, int generateStatus) {
        Map<String, ScheduleSnapshot.Topology.Node> nextNodeIdToObj = scheduleSnapshot.analyzeNextNode(scheduleTopologyNodeId);
        for (String nodeId : nextNodeIdToObj.keySet()) {
            // 找到执行节点
            ScheduleNode scheduleNode = scheduleNodeService.getOne(new QueryWrapper<ScheduleNode>()
                    .eq("schedule_id", scheduleSnapshot.getScheduleId())
                    .eq("schedule_topology_node_id", nodeId)
            );
            scheduleNodeService.generateHistory(scheduleNode, scheduleSnapshot, scheduleInstanceId, generateStatus);
            generateHistory(nodeId, scheduleInstanceId, scheduleSnapshot, generateStatus);
        }
    }


    /**
     * 获取需要触发的执行时间
     * e.g : 每整小时触发一次
     * startDate: 8:30
     * nextTime1: 9:00
     * nextTime2: 10:00
     * interval : 1hour
     * cal1: 8:00
     * cal2: 9:00
     * cal3: 10:00
     * cal4: 11:00
     *
     * @param cron
     * @param startDate
     */
    public static Date getNeedFireTime(String cron,Date startDate){
        Date nextFireTime1 = getNextFireTime(cron,startDate);
        Date nextFireTime2 = getNextFireTime(cron , nextFireTime1);
        // 计算出两次执行时间间隔
        int intervals = (int)(nextFireTime2.getTime() - nextFireTime1.getTime());
        Date cal1= DateUtils.addMilliseconds(nextFireTime1,-intervals);
        Date cal2 = getNextFireTime(cron,cal1);
        Date cal3= getNextFireTime(cron ,cal2);
        if (cal3.equals(nextFireTime1)){
            return cal2;
        }else{
            Date cal4 = getNextFireTime(cron,cal3);
            while (cal4.compareTo(nextFireTime1) > 0){
                cal1 = org.apache.commons.lang.time.DateUtils.addMilliseconds(cal1, - intervals);
                cal2 = getNextFireTime(cron, cal1);
                cal3 = getNextFireTime(cron, cal2);
                cal4 = getNextFireTime(cron, cal3);
            }
            if (cal4.equals(nextFireTime1)) {
                return cal3;
            }
            return cal2;
        }
    }


    /**
     * 获取下次执行时间
     *
     * @param cron      表达式
     * @param startDate 起始时间
     */
    public static Date getNextFireTime(String cron, Date startDate) {
        // 生成cron表达式
        CronScheduleBuilder cronScheduleBuilder = CronScheduleBuilder.cronSchedule(cron);
        // 生成Trigger
        TriggerBuilder<CronTrigger> triggerBuilder = TriggerBuilder.newTrigger().withSchedule(cronScheduleBuilder);
        triggerBuilder.startAt(startDate);

        CronTrigger trigger = triggerBuilder.build();
        return trigger.getFireTimeAfter(startDate);
    }

    /**
     * 启动当前调度实例
     *
     * @param schedule 调度实例
     */
    public static void build(Schedule schedule) {
        SchedulerUtils.scheduleCronJob(
                ScheduleJob.class,
                schedule.getId(),
                SystemConstants.JobGroup.SCHEDULE,
                schedule.generateCron(),
                null,
                schedule.getStartTime(),
                schedule.getEndTime()
        );
    }


}
