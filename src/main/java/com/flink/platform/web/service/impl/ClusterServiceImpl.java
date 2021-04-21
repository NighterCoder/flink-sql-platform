package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.Cluster;
import com.flink.platform.web.mapper.ClusterMapper;
import com.flink.platform.web.service.ClusterService;
import org.springframework.stereotype.Service;

/**
 * Created by 凌战 on 2021/4/21
 */
@Service
public class ClusterServiceImpl extends ServiceImpl<ClusterMapper, Cluster> implements ClusterService {
}
