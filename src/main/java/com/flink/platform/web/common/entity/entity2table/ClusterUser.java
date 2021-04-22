package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 集群使用用户
 *
 * Created by 凌战 on 2021/4/21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("cluster_user")
public class ClusterUser {

    private Integer id;
    private Integer clusterId;
    private Integer userId;

    /**
     * 多个队列使用分隔符,进行分割
     */
    private String queue;
    private String user;

}
