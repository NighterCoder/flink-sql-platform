package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.*;
import com.flink.platform.web.common.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 节点执行历史
 * <p>
 * 这里的节点有不同的类型:
 * 如flink sql,spark sql,flink jar,spark jar等
 * <p>
 * Created by 凌战 on 2021/4/22
 */
@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("node_execute_history")
public class NodeExecuteHistory {

    @TableId(type = IdType.AUTO) // 主键自增
    private Integer id;

    /**
     * 调度id
     */
    private Integer scheduleId;

    /**
     * 拓扑节点id
     */
    private String scheduleTopologyNodeId;

    /**
     * 调度快照id
     */
    private Integer scheduleSnapshotId;

    private String scheduleInstanceId;
    private Integer scheduleRetryNum;

    /**
     * 执行模式:
     * 1.retry 重试
     * 2.rerun 重跑
     * 3.supplement 补数
     */
    private String scheduleHistoryMode;
    /**
     * 调度的历史时间,是提前算出来的
     */
    private Date scheduleHistoryTime;

    /**
     * 监控
     */
    private Integer monitorId;

    /**
     * 节点id
     * <p>
     * 拿到节点id去获取当前节点执行命令可以去执行
     */
    private Integer nodeId;
    /**
     * 节点类型
     */
    private String nodeType;

    // 不需要代理示例,即ip:port
    // private Integer agentId;

    private Integer clusterId;

    /**
     * 执行超时时间
     */
    private Integer timeout;

    private String content; // ????

    private String outputs; // ????

    /**
     * 运行失败的任务的错误日志
     */
    private String errors;

    private Integer createBy;

    /**
     * 当前节点当前执行历史的开始时间
     */
    private Date startTime;
    /**
     * 当前节点当前执行历史的结束时间
     */
    private Date finishTime;

    private String state;

    private String steps; // 记录所有状态


    /**
     * for spark or flink job
     */
    private String jobId;
    private String jobUrl;
    private String jobFinalStatus;


    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private Date updateTime;


    public void updateState(String state) {
        this.state = state;
        if (this.steps == null) {
            this.steps = "[\"" + state + "\"]";
        } else {
            if (!this.steps.contains(state)) {
                this.steps = this.steps.split("]")[0] + ",\"" + state + "\"]";
            }
        }
    }

    public boolean isRunning(){
        return SystemConstants.JobState.WAITING_PARENT_.equals(state) ||
                SystemConstants.JobState.INITED.equals(state) ||
                SystemConstants.JobState.SUBMITTING.equals(state) ||
                SystemConstants.JobState.SUBMITTED.equals(state) ||
                SystemConstants.JobState.ACCEPTED.equals(state) ||
                SystemConstants.JobState.RUNNING.equals(state);
    }

}
