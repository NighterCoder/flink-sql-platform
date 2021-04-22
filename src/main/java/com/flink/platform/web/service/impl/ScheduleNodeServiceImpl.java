package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobNode;
import com.flink.platform.web.mapper.ScheduleNodeMapper;
import com.flink.platform.web.service.ScheduleNodeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class ScheduleNodeServiceImpl extends ServiceImpl<ScheduleNodeMapper, ScheduleNode> implements ScheduleNodeService {


    @Autowired
    private ScheduleNodeService scheduleNodeService;


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
            List<ScheduleNode> nodes = scheduleNodeService.list(new QueryWrapper<ScheduleNode>().eq("cluster_id", node.getClusterId()));
            nodes.forEach(scheduleNode -> queueAndApps.add(
                    scheduleNode.getUser() + "$" + scheduleNode.getQueue() + "$" + scheduleNode.getApp()
            ));


        }


        return null;
    }


}
