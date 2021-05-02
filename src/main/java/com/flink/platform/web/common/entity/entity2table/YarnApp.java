package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 提交到集群上的app,保存到数据库中
 *
 * Created by 凌战 on 2021/4/21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("yarn_app")
public class YarnApp {


    /**
     * id的几种策略: 1.uuid 2.自增id 3.雪花算法 4.redis 5.zookeeper 等
     */

    @TableId(type = IdType.AUTO) // 需要数据库中设置主键自增
    private Integer id;
    /**
     * 集群id
     */
    private Integer clusterId;
    /**
     * 用户id
     */
    private Integer userId;
    /**
     * application Id
     */
    private String appId;
    /**
     * 提交任务到yarn上的用户
     */
    private String user;
    /**
     * 应用名称
     */
    private String name;
    /**
     * 提交到yarn上的队列
     */
    private String queue;
    /**
     * 应用状态
     */
    private String state;
    /**
     * 应用最终状态
     */
    private String finalStatus;
    /**
     * Application跳转url
     */
    private String trackingUrl;
    /**
     * 应用类型:spark/flink
     */
    private String applicationType;
    private Date startedTime;
    private Integer allocatedMb;
    private Date refreshTime;

    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private Date updateTime;
}
