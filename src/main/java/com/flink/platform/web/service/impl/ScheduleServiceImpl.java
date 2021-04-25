package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.mapper.ScheduleMapper;
import com.flink.platform.web.service.ScheduleService;
import com.flink.platform.web.utils.SchedulerUtils;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleServiceImpl extends ServiceImpl<ScheduleMapper, Schedule> implements ScheduleService {


    /**
     * 增加/更新 schedule 实例
     *
     * @param schedule
     * @param scheduleNodes
     */
    @Override
    public void update(Schedule schedule, List<ScheduleNode> scheduleNodes) {
        /**
         * 更新schedule实例时,先暂停Job运行
         */
        if (schedule.getId() != null) {
            SchedulerUtils.pauseJob(schedule.getId(), SystemConstants.JobGroup.SCHEDULE);
        }
        boolean snapshotNew = false;
        boolean cronUpdate = false;

        /**
         * 更新操作
         */
        if (schedule.getId() != null) {
            Schedule dbSchedule = getById(schedule.getId());
            if (!schedule.generateCron().equals(dbSchedule.generateCron())) {
                snapshotNew = true;
                cronUpdate = true;
            }
            if (!schedule.getStartTime().equals(dbSchedule.getStartTime()) ||
                    !schedule.getEndTime().equals(dbSchedule.getEndTime())) {
                cronUpdate = true;
            }
            if (!schedule.getEnabled().equals(dbSchedule.getEnabled())) {
                cronUpdate = true;
            }

            /**
             * 拓扑图发生变化
             */
            if (!schedule.generateCompareTopology().equals(dbSchedule.generateCompareTopology())) {
                snapshotNew = true;
            }
        } else {
            /**
             * 新增操作,均为true
             */
            snapshotNew = true;
            cronUpdate = true;
        }

        /**
         * 调度开启
         */
        if (schedule.getEnabled()) {

        }


    }


}
