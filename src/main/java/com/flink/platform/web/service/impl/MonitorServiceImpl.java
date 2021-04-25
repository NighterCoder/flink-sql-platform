package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.entity2table.Monitor;
import com.flink.platform.web.mapper.MonitorMapper;
import com.flink.platform.web.service.MonitorService;
import org.springframework.stereotype.Service;

/**
 * Created by 凌战 on 2021/4/25
 */
@Service
public class MonitorServiceImpl extends ServiceImpl<MonitorMapper, Monitor> implements MonitorService {
}
