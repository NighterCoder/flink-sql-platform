package com.flink.platform.web.job;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import org.apache.commons.lang3.time.DateUtils;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 清理节点执行历史实例
 * <p>
 * Created by 凌战 on 2021/4/30
 */
@DisallowConcurrentExecution
public class NodeExecuteHistoryClearJob implements Job {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private NodeExecuteHistoryService nodeExecuteHistoryService;

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        // 查找出距今3个月的节点执行历史
        Date before = DateUtils.addMonths(new Date(), -3);
        // 查找出创建时间小于create_time的所有节点执行历史
        List<NodeExecuteHistory> nodeExecuteHistories = nodeExecuteHistoryService.list(new QueryWrapper<NodeExecuteHistory>()
                .lt("create_time", DATE_FORMAT.format(before))
        );
        // 去掉仍然在运行的,例如实时任务
        nodeExecuteHistories.removeIf(NodeExecuteHistory::isRunning);
        nodeExecuteHistoryService.removeByIds(
                nodeExecuteHistories.stream()
                        .map(NodeExecuteHistory::getId)
                        .collect(Collectors.toList())
        );
    }


}
