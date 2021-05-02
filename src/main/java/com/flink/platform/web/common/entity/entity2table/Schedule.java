package com.flink.platform.web.common.entity.entity2table;

import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.annotation.FieldFill;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableName;
import com.flink.platform.web.common.SystemConstants;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.*;

/**
 * Created by 凌战 on 2021/3/26
 */

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "schedule")
public class Schedule {

    private Integer id;
    private String name;
    private String description;
    private Integer cycle;
    private Integer intervals;
    private Integer minute;
    private Integer hour;

    /**
     * 多条数据 用","分割
     */
    private String dayOfWeek;

    private String cron;
    private Date startTime;
    private Date endTime;
    private String topology;
    private Boolean sendEmail;

    /**
     * 多条数据 用","分割
     */
    private String dingdingHooks;
    private Boolean enabled;
    private Date realFireTime;
    private Date needFireTime;
    private Date nextFireTime;
    private String createBy;
    private String updateBy;


    private String keyword;

    @TableField(fill = FieldFill.INSERT)
    private Date createTime;
    @TableField(fill = FieldFill.INSERT_UPDATE)
    private Date updateTime;

    /**
     * 生成定时表达式
     *
     * 0 0/5 * * * ? : 秒 / 分 / 时 / 天(月) / 月 / 天(周) / 年份(一般省略)
     * *表示任何值 ; /表示数值的增量 ; ?只用于天(月)或者天(周),表示设定周
     *
     */
    public String generateCron(){
        if(cron != null){
            return cron;
        }else{
            String cron = null;
            // 根据固定选择生成cron表达式
            if(SystemConstants.TIMER_CYCLE_MINUTE == cycle){
                cron = "0 */" + intervals +" * * * ? *";
            }else if(SystemConstants.TIMER_CYCLE_HOUR == cycle){
                cron = "0 "+minute+" * * * ? *";
            }else if(SystemConstants.TIMER_CYCLE_DAY == cycle){
                cron = "0 "+minute+" "+hour+" * * ? *";
            }else if(SystemConstants.TIMER_CYCLE_WEEK == cycle){
                // dayOfWeek 用,隔开正好符合cron表达式的格式 1,2,4表示每星期的第1,2,4天
                cron = "0 "+minute+" "+hour+" ? * "+dayOfWeek+" *";
            }

            if(cron == null){
                throw new IllegalArgumentException("cron expression is incorrect");
            }

            return cron;
        }
    }


    public String generateCompareTopology() {
        ScheduleSnapshot.Topology top = JSON.parseObject(topology, ScheduleSnapshot.Topology.class);
        List<Map<String, String>> nodes = new ArrayList<>();
        List<Map<String, String>> lines = new ArrayList<>();
        top.nodes.forEach(node -> {
            Map<String, String> nodeMap = new HashMap<>();
            nodeMap.put("id", node.id);
            nodes.add(nodeMap);
        });
        top.lines.forEach(line -> {
            Map<String, String> lineMap = new HashMap<>();
            lineMap.put("id", line.id);
            lineMap.put("fromId", line.fromId());
            lineMap.put("toId", line.toId());
            lines.add(lineMap);
        });
        nodes.sort(Comparator.comparing(node -> node.get("id")));
        lines.sort(Comparator.comparing(line -> line.get("id")));
        Map<String, Object> topMap = new HashMap<>();
        topMap.put("nodes", nodes);
        topMap.put("lines", lines);
        return JSON.toJSONString(topMap);
    }

}
