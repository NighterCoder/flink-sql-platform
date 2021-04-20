package com.flink.platform.web.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flink.platform.web.common.entity.Cluster;
import org.springframework.stereotype.Repository;

/**
 * 继承基本类BaseMapper
 * 所有的CRUD操作已完成,不需要写对应实现的xml
 *
 * 但是特殊复杂的SQL还是需要自己写
 */
@Repository //代表持久层
public interface ClusterMapper extends BaseMapper<Cluster> {
}
