package com.flink.platform.web.common.entity.scheduling;

import lombok.Data;

import java.util.Date;

/**
 * 定时任务WorkFlow中的运行节点
 *
 * Created by 凌战 on 2021/3/26
 */
@Data
public class SchedulingJobNode {

    private String id;
    private String label;
    private String description;

    /**
     * 所属集群id
     */
    private Integer clusterId;


    /**
     * 节点类型:Flink/Spark/Start Node/End Node
     * todo 创建对应枚举类
     */
    private Integer jobType;

    /**
     * 执行类型:Jar/SQL
     * todo 创建对应枚举类
     */
    private Integer executeType;

    /**
     * 如果是SQL执行类型,保存的SQL语句
     */
    private String statement;

    /**
     * 如果是Jar包执行类型,相关配置信息如资源使用,主方法类,参数等
     */
    private String jarConfig;


    /**
     * 创建及更新信息
     */
    private String createBy;
    private Date createTime;
    private String updateBy;
    private Date updateTime;



}
