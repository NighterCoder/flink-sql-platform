package com.flink.platform.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobNode;

/**
 * Created by 凌战 on 2021/4/22
 */
public interface ScheduleNodeService extends IService<ScheduleNode> {

    String validate(SchedulingJobNode node);


}
