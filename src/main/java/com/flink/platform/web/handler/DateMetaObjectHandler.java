package com.flink.platform.web.handler;

import com.baomidou.mybatisplus.core.handlers.MetaObjectHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.reflection.MetaObject;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

/**
 * 在数据库中不配置create_time和update_time为更新和自动更新
 *
 * Created by 凌战 on 2021/4/22
 */
@Slf4j
@Component
public class DateMetaObjectHandler implements MetaObjectHandler {

    @Override
    public void insertFill(MetaObject metaObject) {
        log.info("start insert field....");
        this.setFieldValByName("createTime", LocalDateTime.now(), metaObject);
        this.setFieldValByName("updateTime", LocalDateTime.now(), metaObject);

    }

    @Override
    public void updateFill(MetaObject metaObject) {
        log.info("start update field....");
        this.setFieldValByName("updateTime", LocalDateTime.now(), metaObject);
    }
}
