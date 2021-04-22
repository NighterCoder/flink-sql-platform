package com.flink.platform.web.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.flink.platform.web.common.entity.entity2table.ClusterUser;
import com.flink.platform.web.mapper.ClusterUserMapper;
import com.flink.platform.web.service.ClusterUserService;
import org.springframework.stereotype.Service;

/**
 * Created by 凌战 on 2021/4/21
 */
@Service
public class ClusterUserServiceImpl extends ServiceImpl<ClusterUserMapper, ClusterUser> implements ClusterUserService {
}
