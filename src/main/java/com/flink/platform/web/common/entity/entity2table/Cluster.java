package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName("cluster")
public class Cluster {

    private Integer id;
    private String name;
    private String yarnUrl;
    private String fsDefaultFs;
    private String fsWebhdfs;
    private String fsUser;
    private String fsDir;
    private String defaultFileCluster;
    private Boolean flinkProxyUserEnabled;
    private String streamBlackNodeList;
    private String batchBlackNodeList;


}
