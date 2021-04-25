package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleService;
import com.flink.platform.web.service.ScheduleSnapshotService;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;

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
        scheduleService.save(schedule);

        // 根据scheduleId和计划触发时间查找快照
        ScheduleSnapshot scheduleSnapshot = scheduleSnapshotService.getOne(
                new QueryWrapper<ScheduleSnapshot>().eq("schedule_id", scheduleId)
                        .eq("snapshot_time", jobExecutionContext.getScheduledFireTime())
        );
        if (scheduleSnapshot == null) {
            return;
        }
        confirmedNeed(jobExecutionContext, scheduleSnapshot);
    }


    private void confirmedNeed(JobExecutionContext jobExecutionContext, ScheduleSnapshot scheduleSnapshot) {
        // 调度实例id由计划执行时间转换得来
        String scheduleInstanceId = dateFormat.format(jobExecutionContext.getScheduledFireTime());
        List<NodeExecuteHistory> executeHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .eq("schedule_id", scheduleSnapshot.getScheduleId())
                .eq("schedule_instance_id", scheduleInstanceId)
                .eq("state", SystemConstants.JobState.UN_CONFIRMED_)
        );
        // 没有计划执行时间的执行历史
        if(executeHistoryList.isEmpty()){
            // 去查找下次触发时间对应的调度节点历史
            scheduleInstanceId = dateFormat.format(jobExecutionContext.getNextFireTime());
            executeHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .eq("schedule_id",scheduleSnapshot.getScheduleId())
                .eq("schedule_instance_id",scheduleInstanceId)
            );
            // 下次触发时间对应的调度,对应节点执行历史为空,
            if (executeHistoryList.isEmpty()){
                executeHistoryList = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                        .eq("schedule_id",scheduleSnapshot.getScheduleId())
                        .eq("state", SystemConstants.JobState.UN_CONFIRMED_)
                );
                executeHistoryList.forEach(nodeExecuteHistory -> nodeExecuteHistoryService.missingScheduling(nodeExecuteHistory));


            }




        }




    }


}
