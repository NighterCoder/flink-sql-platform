package com.flink.platform.web.controller.schedule;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.flink.platform.web.common.entity.Msg;
import com.flink.platform.web.common.entity.entity2table.Schedule;
import com.flink.platform.web.common.entity.entity2table.ScheduleNode;
import com.flink.platform.web.common.entity.login.LoginUser;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobDto;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobNode;
import com.flink.platform.web.controller.BaseController;
import com.flink.platform.web.service.ScheduleNodeService;
import com.flink.platform.web.service.ScheduleService;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * 定时任务WorkFlow Controller
 * <p>
 * todo requestMapping最后需要根据权限统一更改
 * <p>
 * Created by 凌战 on 2021/3/26
 */

@RestController
@RequestMapping("/schedule")
public class ScheduleController extends BaseController {

    @Autowired
    private ScheduleService scheduleService;

    @Autowired
    private ScheduleNodeService scheduleNodeService;


    /**
     * 保存或者更新定时调度任务
     * 1. 更新调度任务中所有执行节点的信息
     * 2. 对执行节点进行资源检查,重复提交检测等
     * 3. 更新调度任务的相关信息
     *
     * @param req 定时调度任务实体类
     */
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    public Msg save(@RequestBody SchedulingJobDto req) {
        String msg = req.validate();
        if (msg != null) {
            return failed(msg);
        }
        // 当前用户
        LoginUser loginUser = getCurrentUser();
        // 操作时间
        Date now = new Date();

        // 遍历WorkFlow中的任务节点
        /**
         * 更新所有nodes的相关信息
         */
        for (SchedulingJobNode node : req.getJobNodes()) {
            // 新创建的节点,id为空
            if (node.getId() == null) {
                node.setCreateBy(loginUser.getUsername());
                node.setCreateTime(now);
            }
            node.setUpdateBy(loginUser.getUsername());
            node.setUpdateTime(now);
        }

        // todo 增加资源校验等等
        /**
         * 1. 对调度任务中每一个节点进行校验
         */
        for (SchedulingJobNode node : req.getJobNodes()) {
            msg = scheduleNodeService.validate(node);
            if (msg != null) {
                return failed("执行节点【" + node.getName() + "】" + msg);
            }
        }

        /**
         * 2. 当前重复执行节点检查
         * 对重复执行任务节点的判断:clusterId$user$queue$app,这里的user是指提交到yarn的user(与createBy区分)
         */
        Set<String> keys = new HashSet<>();
        for (SchedulingJobNode node : req.getJobNodes()) {
            if (node.isYarn()) {
                String key = node.getClusterId() + "$" + node.getUser() + "$" + node.getQueue() + "$" + node.getApp();
                if (keys.contains(key)) {
                    return failed("执行节点【" + node.getName() + "】Yarn应用重复");
                }
                keys.add(key);
            }
        }

        if (req.getId() == null) {
            Schedule dbSchedule = scheduleService.getOne(
                    new QueryWrapper<Schedule>().eq("name", req.getName())
            );
            if (dbSchedule != null) {
                return failed("调度【" + req.getName() + "】已存在");
            }
            req.setCreateBy(loginUser.getId());
            req.setCreateTime(now);
        } else {
            /**
             * 更新当前调度,检查是否存在调度名称相同但是id不相同的另外一个调度任务
             */
            Schedule dbSchedule = scheduleService.getOne(
                    new QueryWrapper<Schedule>().eq("name", req.getName())
                            .ne("id", req.getId())
            );
            if (dbSchedule != null) {
                return failed("调度【" + req.getName() + "】已存在");
            }
        }
        req.setUpdateBy(loginUser.getId());
        req.setUpdateTime(now);

        Schedule schedule = new Schedule();
        BeanUtils.copyProperties(req, schedule); // 拷贝相同属性
        if (req.getDayOfWeek() != null && !req.getDayOfWeek().isEmpty()) {
            schedule.setDayOfWeek(StringUtils.join(req.getDayOfWeek(), ","));
        }
        if (req.getDingdingHooks() != null && !req.getDingdingHooks().isEmpty()) {
            schedule.setDingdingHooks(StringUtils.join(req.getDingdingHooks(), ","));
        }
        StringBuilder keyword = new StringBuilder(req.getName());
        List<ScheduleNode> scheduleNodes = new ArrayList<>();
        for (SchedulingJobNode node : req.getJobNodes()) {
            ScheduleNode scheduleNode = new ScheduleNode();
            BeanUtils.copyProperties(node, scheduleNode);
            scheduleNodes.add(scheduleNode);
            keyword.append("_").append(node.getName()).append("_").append(node.getApp());
        }
        schedule.setKeyword(keyword.toString().replaceAll("_null", "_-"));

        //scheduleService.update()
        return success();
    }


}
