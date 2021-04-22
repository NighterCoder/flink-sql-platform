package com.flink.platform.web.controller.schedule;

import com.flink.platform.web.common.entity.Msg;
import com.flink.platform.web.common.entity.login.LoginUser;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobDto;
import com.flink.platform.web.common.entity.scheduling.SchedulingJobNode;
import com.flink.platform.web.controller.BaseController;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

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

    /**
     * 保存或者更新定时调度任务
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
        for (SchedulingJobNode node:req.getJobNodes()){
            // 新创建的节点,id为空
            if(node.getId() == null){
                node.setCreateBy(loginUser.getUsername());
                node.setCreateTime(now);
            }
            node.setUpdateBy(loginUser.getUsername());
            node.setUpdateTime(now);
        }

        // todo 增加资源校验等等
        for (SchedulingJobNode node:req.getJobNodes()){

        }








        return null;
    }


}
