package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.entity2table.ScheduleSnapshot;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobNode;
import com.flink.platform.web.mapper.ScheduleNodeMapper;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import com.flink.platform.web.service.ScheduleNodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleNodeServiceImpl extends ServiceImpl<ScheduleNodeMapper, ScheduleNode> implements ScheduleNodeService {

    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;


    /**
     * 校验当前执行节点的相关信息:
     * 1.检查是否是重复提交的执行节点
     * 2....
     *
     * @param node
     */
    @Override
    public String validate(SchedulingJobNode node) {
        // 新创建的执行节点
        if (node.getId() == null) {
            // 检查yarn应用名称是否重复
            Set<String> queueAndApps = new HashSet<>();
            // 查询提交到当前集群的所有执行节点
            List<ScheduleNode> nodes = list(new QueryWrapper<ScheduleNode>().eq("cluster_id", node.getClusterId()));
            nodes.forEach(scheduleNode -> queueAndApps.add(
                    scheduleNode.getUser() + "$" + scheduleNode.getQueue() + "$" + scheduleNode.getApp()
            ));


        }


        return null;
    }

    @Override
    public boolean execute(ScheduleNode scheduleNode, Monitor monitor) {
        return false;
    }


    /**
     * 生成执行历史
     *
     * @param scheduleNode
     * @param monitor
     * @param scheduleSnapshot   为空的话,生成节点执行历史,
     * @param scheduleInstanceId 调度历史实例id
     * @param generateStatus     0:需确认  1:确认  2:补数
     */
    private NodeExecuteHistory generateHistory(ScheduleNode scheduleNode,
                                               Monitor monitor,
                                               ScheduleSnapshot scheduleSnapshot,
                                               String scheduleInstanceId,
                                               Integer generateStatus) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        // 后缀
        String suffix;
        if (scheduleSnapshot != null) {
            suffix = scheduleInstanceId;
        } else {
            suffix = dateFormat.format(new Date());
        }
        // 获取最近一次快照的日期
        Date date;
        try {
            date = dateFormat.parse(suffix);
        } catch (ParseException e) {
            throw new RuntimeException("Error date format: " + suffix);
        }

        NodeExecuteHistory nodeExecuteHistory;
        /**
         * 确认过的
         */
        if (scheduleSnapshot != null && generateStatus == 1) {
            nodeExecuteHistory = nodeExecuteHistoryService.getOne(new QueryWrapper<NodeExecuteHistory>()
                    .eq("schedule_id", scheduleSnapshot.getScheduleId())
                    .eq("schedule_top_node_id", scheduleNode.getScheduleTopNodeId())
                    .eq("schedule_instance_id", scheduleInstanceId)
                    .eq("state", SystemConstants.JobState.UN_CONFIRMED_));
        } else {
            /**
             *
             */
            nodeExecuteHistory = NodeExecuteHistory.builder()
                    .nodeId(scheduleNode.getId())
                    .nodeType(scheduleNode.getType())
                    .clusterId(scheduleNode.getClusterId())
                    .timeout(scheduleNode.getTimeout())
                    .createTime(date)
                    .createBy(scheduleNode.getCreateBy())
                    .build();
        }
        /**
         * 赋予调度任务相关属性
         */
        if (scheduleSnapshot != null) {
            nodeExecuteHistory.setScheduleId(scheduleSnapshot.getScheduleId());
            nodeExecuteHistory.setScheduleTopNodeId(scheduleNode.getScheduleTopNodeId());
            nodeExecuteHistory.setScheduleSnapshotId(scheduleSnapshot.getId());
            nodeExecuteHistory.setScheduleInstanceId(scheduleInstanceId);
            // 如果是部署
            if (generateStatus == 2) {
                nodeExecuteHistory.setScheduleHistoryMode(SystemConstants.HistoryMode.SUPPLEMENT);
                Date now = new Date();
                nodeExecuteHistory.setScheduleHistoryTime(
                        // 小于当前时间, 则为now
                        // 最新时间作为调度历史时间
                        date.compareTo(now) <= 0 ? now : date
                );
            }
        }

        if (monitor != null) {
            nodeExecuteHistory.setMonitorId(monitor.getId());
        }
        //todo 修改app名称,添加后缀

        // 提交到yarn上 && 执行节点为空
        if (scheduleNode.isYarn() && nodeExecuteHistory.getNodeId() == null) {
            nodeExecuteHistory.setOutputs("proxy user: " + scheduleNode.getUser() + "\n");
        }

        /**
         * 快照为空
         */
        if (scheduleSnapshot == null) {
            nodeExecuteHistory.updateState(SystemConstants.JobState.INITED);
        } else {
            if (generateStatus == 0 || generateStatus == 2) {
                nodeExecuteHistory.updateState(SystemConstants.JobState.UN_CONFIRMED_);
            } else {
                nodeExecuteHistory.updateState(SystemConstants.JobState.WAITING_PARENT_);
            }
        }

        nodeExecuteHistoryService.saveOrUpdate(nodeExecuteHistory);
        return nodeExecuteHistoryService.getById(nodeExecuteHistory.getId());
    }


}
