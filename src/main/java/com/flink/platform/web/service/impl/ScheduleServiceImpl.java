package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.mapper.ScheduleMapper;
import com.flink.platform.web.service.ScheduleService;
import org.springframework.stereotype.Service;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleServiceImpl extends ServiceImpl<ScheduleMapper, Schedule> implements ScheduleService {
}
