package com.flink.platform.web.common.entity.jar;

import lombok.Data;

@Data
public class JarConf {
    private Long id;
    private String jarPath;
    private String jarName;

    /**
     * 自定义任务名称
     */
    private String name;

    /**
     * 主类名称
     */
    private String entryClass;

    //todo

}
