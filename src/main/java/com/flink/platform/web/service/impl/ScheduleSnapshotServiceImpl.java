package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.mapper.ScheduleSnapshotMapper;
import com.flink.platform.web.service.ScheduleSnapshotService;
import org.springframework.stereotype.Service;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleSnapshotServiceImpl extends ServiceImpl<ScheduleSnapshotMapper, ScheduleSnapshot> implements ScheduleSnapshotService {
}
