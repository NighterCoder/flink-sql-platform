package com.flink.platform.web.mapper;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.flink.platform.web.common.entity.analysis.SessionDO;
import org.springframework.stereotype.Repository;

import java.util.HashMap;

/**
 * Created by 凌战 on 2021/2/20
 */
@Repository
public interface SessionMapper extends BaseMapper<SessionDO> {


    default SessionDO selectByConditions(String username, Integer sessionType, Integer executeType) {
        HashMap<String, Object> conditions = new HashMap<>();
        conditions.put("username", username);
        conditions.put("session_type", sessionType);
        conditions.put("execute_type", executeType);
        //会自动加上deleted=0条件
        //conditions.put("deleted",0);
        return selectOne(new QueryWrapper<SessionDO>().allEq(conditions));
    }


    default void deleteSessionByConditions(String username, Integer sessionType, Integer executeType) {
        HashMap<String, Object> conditions = new HashMap<>();
        conditions.put("username", username);
        conditions.put("session_type", sessionType);
        conditions.put("execute_type", executeType);
        deleteByMap(conditions);
    }


}
