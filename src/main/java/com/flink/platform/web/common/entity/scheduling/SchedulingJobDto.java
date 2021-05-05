package com.flink.platform.web.common.entity.scheduling;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.AbstractPageDto;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.Date;
import java.util.List;

/**
 * 定时任务封装实体类
 * WorkFlow: 里面需要有一个属性记录子任务列表
 * <p>
 * Created by 凌战 on 2021/3/26
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class SchedulingJobDto extends AbstractPageDto {

    private String id;
    /**
     * 名字判定调度是否重复
     */
    private String name;
    private String description;

    /**
     * 周期
     */
    private Integer cycle;
    private Integer intervals;
    private Integer minute;
    private Integer hour;
    private List<String> dayOfWeek;

    /**
     * cron表达式
     */
    private String cron;
    private Date startTime;
    private Date endTime;

    /**
     * 拓扑相关
     */
    private String topology;

    /**
     * 告警相关配置
     */
    private Boolean sendEmail;
    private List<String> dingdingHooks;

    /**
     * 任务运行相关
     */
    private Date realFireTime;
    private Date needFireTime;
    private Date nextFireTime;
    private Boolean enabled;


    /**
     * 创建者,更新者
     */
    private String createBy;
    private Date createTime;
    private String updateBy;
    private Date updateTime;

    private String keyword;

    /**
     * WorkFlow中的执行节点列表
     */
    private List<SchedulingJobNode> jobNodes;



    @Override
    public String validate() {

        if(StringUtils.isBlank(topology)){
            return "拓扑不能为空";
        }
        if(startTime == null ||endTime == null){
            return "请选择任务执行时间范围";
        }
        if (cron == null){
            if(this.cycle == SystemConstants.TIMER_CYCLE_MINUTE && intervals == null){
                return "时间间隔不能为空";
            }
            if(this.cycle == SystemConstants.TIMER_CYCLE_HOUR && this.minute == null){
                return "分钟不能为空";
            }
            if(this.cycle == SystemConstants.TIMER_CYCLE_DAY && (this.hour == null || this.minute == null)){
                return "小时、分钟不能为空";
            }
            if (this.cycle == SystemConstants.TIMER_CYCLE_WEEK && (this.dayOfWeek == null || this.hour == null || this.minute == null)) {
                return "天、小时、分钟不能为空";
            }
        }
        return null;
    }







}
