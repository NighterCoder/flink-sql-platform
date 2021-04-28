package com.flink.platform.web.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;

import java.util.Date;

/**
 * Created by 凌战 on 2021/4/22
 */
public interface ScheduleSnapshotService extends IService<ScheduleSnapshot> {

    /**
     * 获取最近的调度快照
     * @param scheduleId
     * @param snapshotTime
     * @return
     */
    ScheduleSnapshot findByScheduleIdAndSnapshotTime(Integer scheduleId, Date snapshotTime);


}
