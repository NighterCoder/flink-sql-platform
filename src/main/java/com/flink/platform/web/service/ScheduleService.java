package com.flink.platform.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;

import java.util.List;

/**
 * Created by 凌战 on 2021/4/22
 */
public interface ScheduleService extends IService<Schedule>  {

    void update(Schedule schedule, List<ScheduleNode> scheduleNodes);

}
