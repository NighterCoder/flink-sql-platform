package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.mapper.NodeExecuteHistoryMapper;
import com.flink.platform.web.service.NodeExecuteHistoryService;
import org.springframework.stereotype.Service;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class NodeExecuteHistoryServiceImpl extends ServiceImpl<NodeExecuteHistoryMapper, NodeExecuteHistory> implements NodeExecuteHistoryService {

    /**
     * 执行节点错过调度
     *
     * @param nodeExecuteHistory
     */
    @Override
    public void missingScheduling(NodeExecuteHistory nodeExecuteHistory) {
        nodeExecuteHistory.updateState(SystemConstants.JobState.FAILED);
        nodeExecuteHistory.setErrors("Missing scheduling");
        saveOrUpdate(nodeExecuteHistory);
    }


}
