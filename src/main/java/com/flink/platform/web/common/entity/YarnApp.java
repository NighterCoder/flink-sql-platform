package com.flink.platform.web.common.entity;

import com.baomidou.mybatisplus.annotation.TableName;
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
    private Integer allocateMB;
    private Date refreshTime;

}
