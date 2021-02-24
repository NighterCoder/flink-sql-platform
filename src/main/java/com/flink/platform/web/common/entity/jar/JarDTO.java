package com.flink.platform.web.common.entity.jar;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;

import java.sql.Timestamp;

/**
 * Created by 凌战 on 2021/2/22
 */
@Data
public class JarDTO {

    private Long id;
    private String jarPath; // jar包存储位置
    private String jarName; // jar包名称
    private String username; // 上传用户
    // jar包相关配置
    private String config;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Shanghai")
    private Timestamp uploadTime;


}
