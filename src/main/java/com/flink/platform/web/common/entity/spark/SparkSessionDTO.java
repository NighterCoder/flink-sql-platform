package com.flink.platform.web.common.entity.spark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * 交互式创建SQL会话
 *
 * 相关参数:http://livy.incubator.apache.org./docs/latest/rest-api.html
 */
@Data
@AllArgsConstructor
@Slf4j
public class SparkSessionDTO {
    private String kind = "sql";
    private String proxyUser = "root";
    private String driverMemory = "200M";
    private Integer driverCores = 1;
    private String executorMemory = "400M";
    private Integer executorCores = 1;
    private Integer numExecutors = 1;
    /**
     * Session的名称
     */
    private String name;
    /**
     * spark的相关配置
     */
    private Map<String, String> conf;


    public SparkSessionDTO(String name,String sparkFiles,Boolean dynamicAllocation){
        this.name = name;
        this.conf = new HashMap<>(1);
        conf.put("spark.files",sparkFiles);

        if (dynamicAllocation){
            log.info("Spark开启动态资源分配服务");
            // 开启动态资源分配
            conf.put("spark.dynamicAllocation.enabled", "true");
            conf.put("spark.dynamicAllocation.maxExecutors", "8");
            // 开启外部shuffle，详情可以了解下 spark external shuffle service
            conf.put("spark.shuffle.service.enabled", "true");
            conf.put("spark.shuffle.service.port", "7338");
        }
    }




}
