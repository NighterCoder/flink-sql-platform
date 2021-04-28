package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.mapper.ScheduleSnapshotMapper;
import com.flink.platform.web.service.ScheduleSnapshotService;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleSnapshotServiceImpl extends ServiceImpl<ScheduleSnapshotMapper, ScheduleSnapshot> implements ScheduleSnapshotService {


    @Override
    public ScheduleSnapshot findByScheduleIdAndSnapshotTime(Integer scheduleId, Date snapshotTime) {
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return list(new QueryWrapper<ScheduleSnapshot>()
                .eq("schedule_id", scheduleId)
                .le("snapshot_time", dateFormat.format(snapshotTime))
                .orderByDesc("snapshot_time")
        ).get(0);
    }


}
