package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.*;
import com.flink.platform.web.common.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 执行节点
 * Created by 凌战 on 2021/4/22
 */
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Data
@TableName("schedule_node")
public class ScheduleNode {

    @TableId(type = IdType.AUTO)
    private Integer id;

    private String name;
    private String description;
    private String type;

    /**
     * 当前执行节点属于哪一个schedule
     */
    private Integer scheduleId;

    /**
     * 拓扑节点ID
     */
    private String scheduleTopologyNodeId;
    private Integer monitorId;

    /**
     * 列表查询字段???
     */
    private Boolean monitorEnabled;

    // 不需要代理示例,即ip:port
    // private Integer agentId;
    private Integer clusterId;
    private Integer timeout;
    /**
     * 所有执行需要的配置
     */
    private String content;
    private String input;
    private String output;

    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private Date modifyTime;
    private Integer createBy;
    private Integer modifyBy;


    /**
     * yarn 应用属性
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
