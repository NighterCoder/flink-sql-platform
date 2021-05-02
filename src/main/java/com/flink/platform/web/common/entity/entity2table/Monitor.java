package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.*;
import com.flink.platform.web.common.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 对应监控的定时任务
 *
 * Created by 凌战 on 2021/4/25
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("monitor")
public class Monitor {

    @TableId(type = IdType.AUTO)
    private Integer id;
    private Integer cycle;
    private Integer intervals;
    private Integer minute;
    private Integer hour;

    /**
     * 多条数据用分隔符,分割
     */
    private String dayOfWeek;
    private String cron;
    private Boolean exRestart;
    private Integer waitingBatches;
    private Boolean blockingRestart;
    private Boolean sendEmail;
    /**
     * 多条数据用,分割
     */
    private String dingdingHooks;
    private Boolean enabled;
    private Date realFireTime;

    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private Date updateTime;


    public String generateCron() {
        if (cron != null) {
            return cron;
        } else {
            String cron = null;
            if (cycle == SystemConstants.TIMER_CYCLE_MINUTE) {
                cron = "0 */" + intervals + " * * * ? *";
            } else if (cycle == SystemConstants.TIMER_CYCLE_HOUR) {
                cron = "0 " + minute + " * * * ? *";
            } else if (cycle == SystemConstants.TIMER_CYCLE_DAY) {
                cron = "0 " + minute + " " + hour + " * * ? *";
            } else if (cycle == SystemConstants.TIMER_CYCLE_WEEK) {
                cron = "0 " + minute + " " + hour + " ? * " + dayOfWeek + " *";
            }
            if (cron == null) {
                throw new IllegalArgumentException("cron expression is incorrect");
            }
            return cron;
        }
    }





}
