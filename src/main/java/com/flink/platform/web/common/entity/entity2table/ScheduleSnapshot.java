package com.flink.platform.web.common.entity.entity2table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.flink.platform.web.common.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

/**
 * 调度拓扑的快照
 * <p>
 * 后续修改调度拓扑,在快照表中添加记录
 * Created by 凌战 on 2021/4/22
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@TableName("schedule_snapshot")
public class ScheduleSnapshot {

    @TableId(type = IdType.AUTO)
    private Integer id;

    private Integer scheduleId;
    private Date snapshotTime;
    private String name;
    private String description;

    private Integer cycle;
    private Integer intervals;
    private Integer minute;
    private Integer hour;

    /**
     * 多条数据使用逗号分割
     */
    private String dayOfWeek;
    private String cron;

    /**
     * 任务开始和结束时间
     */
    private Date startTime;
    private Date endTime;

    /**
     * 拓扑保存时,以JSON形式保存
     * lines 和 nodes
     */
    private String topology;
    private Boolean sendEmail;

    /**
     * 多条数据用,分割
     */
    private String dingdingHooks;
    private Boolean enabled;


    public String generateCron() {
        if (cron != null) {
            return cron;
        } else {
            String cron = null;
            // 根据固定选择生成cron表达式
            if (SystemConstants.TIMER_CYCLE_MINUTE == cycle) {
                cron = "0 */" + intervals + " * * * ? *";
            } else if (SystemConstants.TIMER_CYCLE_HOUR == cycle) {
                cron = "0 " + minute + " * * * ? *";
            } else if (SystemConstants.TIMER_CYCLE_DAY == cycle) {
                cron = "0 " + minute + " " + hour + " * * ? *";
            } else if (SystemConstants.TIMER_CYCLE_WEEK == cycle) {
                // dayOfWeek 用,隔开正好符合cron表达式的格式 1,2,4表示每星期的第1,2,4天
                cron = "0 " + minute + " " + hour + " ? * " + dayOfWeek + " *";
            }

            if (cron == null) {
                throw new IllegalArgumentException("cron expression is incorrect");
            }

            return cron;
        }
    }

    /**
     * 解析当前节点的上一个节点
     * todo 当前只支持单节点上游
     *
     * @param currentNodeId
     * @return
     */
    public Topology.Node analyzePreviousNode(String currentNodeId) {
        Topology top = JSON.parseObject(topology, Topology.class);

        String previousNodeId = null;
        // 遍历line,找到toId为currentNodeId的line
        for (Topology.Line line : top.lines) {
            if (line.toId().equals(currentNodeId)) {
                previousNodeId = line.formId();
                break;
            }
        }
        for (Topology.Node node : top.nodes) {
            if (node.id.equals(previousNodeId)) {
                return node;
            }
        }
        return null;
    }

    /**
     * 解析当前节点
     *
     * @param currentNodeId
     * @return
     */
    public Topology.Node analyzeCurrentNode(String currentNodeId) {
        Topology top = JSON.parseObject(topology, Topology.class);
        for (Topology.Node node : top.nodes) {
            if (node.id.equals(currentNodeId)) {
                return node;
            }
        }
        return null;
    }

    /**
     * 解析当前节点的下游节点
     * 支持多节点下游
     *
     * @param currentNodeId 可以为null
     * @return
     */
    public Map<String, Topology.Node> analyzeNextNode(String currentNodeId) {
        Map<String, Topology.Node> nodeIdToObj = new HashMap<>();
        Topology top = JSON.parseObject(topology, Topology.class);
        top.nodes.forEach(node -> nodeIdToObj.put(node.id, node));

        List<String> toIds = new ArrayList<>();
        top.lines.forEach(line -> toIds.add(line.toId()));

        // 找到rootNodeId
        String rootNodeId = null;
        for (Topology.Node node : top.nodes) {
            // 当前节点不是某条Line的toId
            if (!toIds.contains(node.id)) {
                rootNodeId = node.id;
                break;
            }
        }
        /**
         * 参数为null,返回根节点
         */
        if (currentNodeId == null) {
            return Collections.singletonMap(rootNodeId, nodeIdToObj.get(rootNodeId));
        } else {
            nodeIdToObj.remove(rootNodeId);
            top.lines.forEach(line -> {
                // 挨个移除
                if (!line.formId().equals(currentNodeId)) {
                    nodeIdToObj.remove(line.toId());
                }
            });
            return nodeIdToObj;
        }
    }


    public static class Topology {
        public List<Node> nodes;
        public List<Line> lines;

        /**
         * 节点
         */
        public static class Node {
            public String id;
            public JSONObject data;

            public Node(String id, JSONObject data) {
                this.id = id;
                this.data = data;
            }

            // 重试次数
            public int retries() {
                return data.getIntValue("retries");
            }

            // 重试间隔
            public int intervals() {
                return data.getIntValue("intervals");
            }

        }


        /**
         * 节点之间的连接线
         */
        public static class Line {
            public String id;
            public JSONObject from;
            public JSONObject to;

            public Line(String id, JSONObject from, JSONObject to) {
                this.id = id;
                this.from = from;
                this.to = to;
            }


            public String formId() {
                return from.getString("id");
            }

            public String toId() {
                return to.getString("id");
            }
        }

    }


}
