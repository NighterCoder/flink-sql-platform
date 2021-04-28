package com.flink.platform.web.common.entity.scheduling;

import com.flink.platform.web.common.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 定时任务WorkFlow中的运行节点
 * <p>
 * Created by 凌战 on 2021/3/26
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SchedulingJobNode {

    private String id;
    private String name;
    private String description;

    /**
     * 所属集群id
     */
    private Integer clusterId;

    /**
     * 所属的调度任务id
     */
    private Integer scheduleId;

    /**
     * 拓扑节点ID
     */
    private String scheduleTopologyNodeId;
    private Integer monitorId;

    /**
     * 节点类型:Flink/Spark/
     * todo 创建对应枚举类
     */
    private String type;


    private String timeout;
    /**
     * 如果是SQL执行类型,保存的SQL语句
     */
    private String content;
    private String input;
    private String output;

    /**
     * 如果是Jar包执行类型,相关配置信息如资源使用,主方法类,参数等
     */
    @Deprecated
    private String jarConfig;


    /**
     * 创建及更新信息
     */
    private String createBy;
    private Date createTime;
    private String updateBy;
    private Date updateTime;

    /**
     * yarn信息
     */
    private String user;
    private String queue;
    private String app;


    public boolean isBatch() {
        return SystemConstants.NodeType.FLINK_BATCH_SQL.equals(type) ||
                SystemConstants.NodeType.SPARK_BATCH_JAR.equals(type) ||
                SystemConstants.NodeType.SPARK_BATCH_SQL.equals(type);
    }

    public boolean isStream() {
        return SystemConstants.NodeType.FLINK_STREAM_JAR.equals(type) ||
                SystemConstants.NodeType.FLINK_STREAM_SQL.equals(type);
    }


    public boolean isYarn() {
        return SystemConstants.NodeType.FLINK_BATCH_SQL.equals(type) ||
                SystemConstants.NodeType.SPARK_BATCH_JAR.equals(type) ||
                SystemConstants.NodeType.SPARK_BATCH_SQL.equals(type) ||
                SystemConstants.NodeType.FLINK_STREAM_JAR.equals(type) ||
                SystemConstants.NodeType.FLINK_STREAM_SQL.equals(type);
    }


}
