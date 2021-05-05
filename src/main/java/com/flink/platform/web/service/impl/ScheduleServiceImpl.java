package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.job.ScheduleJob;
import com.flink.platform.web.mapper.ScheduleMapper;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.service.ScheduleService;
import com.flink.platform.web.service.ScheduleSnapshotService;
import com.flink.platform.web.utils.SchedulerUtils;
import javafx.scene.Node;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleServiceImpl extends ServiceImpl<ScheduleMapper, Schedule> implements ScheduleService {

    @Autowired
    private ScheduleSnapshotService scheduleSnapshotService;

    @Autowired
    private ScheduleNodeService scheduleNodeService;

    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;

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

        Date now = new Date();
        /**
         * 调度开启
         */
        if (schedule.getEnabled()) {
            Date needFireTime = ScheduleJob.getNeedFireTime(schedule.generateCron(), schedule.getStartTime().compareTo(now) <= 0 ? now : schedule.getStartTime());
            Date nextFireTime = ScheduleJob.getNextFireTime(schedule.generateCron(), schedule.getStartTime().compareTo(now) <= 0 ? now : schedule.getStartTime());
            schedule.setNeedFireTime(needFireTime);
            schedule.setNextFireTime(nextFireTime);
        }else{
            schedule.setNeedFireTime(null);
            schedule.setNextFireTime(null);
        }
        // schedule已经变成了保存或更新数据后
        super.saveOrUpdate(schedule);

        ScheduleSnapshot scheduleSnapshot;
        if (snapshotNew){
            scheduleSnapshot = new ScheduleSnapshot();
            BeanUtils.copyProperties(schedule,scheduleSnapshot);
            scheduleSnapshot.setId(null);
            scheduleSnapshot.setScheduleId(schedule.getId());
            scheduleSnapshot.setSnapshotTime(new Date());
            scheduleSnapshotService.saveOrUpdate(scheduleSnapshot);
        }else{
            // 更新旧快照
            scheduleSnapshot = scheduleSnapshotService.findByScheduleIdAndSnapshotTime(schedule.getId(),now);
            Integer oldId = scheduleSnapshot.getId();
            BeanUtils.copyProperties(schedule,scheduleSnapshot);
            scheduleSnapshot.setId(oldId);
            scheduleSnapshotService.saveOrUpdate(scheduleSnapshot);
        }

        // 保存调度中的各个节点
        for (ScheduleNode scheduleNode:scheduleNodes){
            if (scheduleNode.getScheduleId() == null){
                scheduleNode.setScheduleId(schedule.getId());
            }
            scheduleNodeService.saveOrUpdate(scheduleNode);
        }

        // 新增快照或者调度cron修改
        if (cronUpdate || snapshotNew){
            if (schedule.getEnabled()){
                DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                String scheduleInstanceId = dateFormat.format(schedule.getNextFireTime());
                // 查询节点执行历史
                List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                    .eq("schedule_id",scheduleSnapshot.getScheduleId())
                    .eq("state",SystemConstants.JobState.UN_CONFIRMED_)
                );
                dealHistory(null,scheduleInstanceId,scheduleSnapshot,0,nodeExecuteHistories);
                nodeExecuteHistories.forEach(nodeExecuteHistoryService::missingScheduling);
            }else{
                // 停用调度
                List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                        .eq("schedule_id",scheduleSnapshot.getScheduleId())
                        .eq("state",SystemConstants.JobState.UN_CONFIRMED_)
                );
                nodeExecuteHistories.forEach(nodeExecuteHistoryService::missingScheduling);
            }
            SchedulerUtils.deleteJob(schedule.getId(),SystemConstants.JobGroup.SCHEDULE);
            // build
            if (schedule.getEnabled()){
                ScheduleJob.build(schedule);
            }
        }else{
            if (schedule.getId() != null){
                SchedulerUtils.resumeJob(schedule.getId(),SystemConstants.JobGroup.SCHEDULE);
            }
        }


    }


    private void dealHistory(String scheduleTopologyNodeId,String scheduleInstanceId,ScheduleSnapshot scheduleSnapshot,
                             int generateStatus,List<NodeExecuteHistory> nodeExecuteHistories){
        /**
         * 当前节点的下节点
         */
        Map<String, ScheduleSnapshot.Topology.Node> nextNodeIdToObj = scheduleSnapshot.analyzeNextNode(scheduleTopologyNodeId);
        for (String nodeId:nextNodeIdToObj.keySet()){
            boolean exist = nodeExecuteHistories.removeIf(nodeExecuteHistory -> nodeExecuteHistory.getScheduleTopologyNodeId().equals(nodeId) &&
                     nodeExecuteHistory.getScheduleInstanceId().equals(scheduleInstanceId));
            if (!exist){
                ScheduleNode scheduleNode = scheduleNodeService.getOne(new QueryWrapper<ScheduleNode>()
                        .eq("schedule_id",scheduleSnapshot.getScheduleId())
                        .eq("schedule_topology_node_id",nodeId)
                );
                scheduleNodeService.generateHistory(scheduleNode,scheduleSnapshot,scheduleInstanceId,generateStatus);
            }
            dealHistory(nodeId,scheduleInstanceId,scheduleSnapshot,generateStatus,nodeExecuteHistories);
        }
    }



}
