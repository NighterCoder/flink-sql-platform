package com.flink.platform.web.common.entity;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.Properties;

/**
 * Created by 凌战 on 2021/5/11
 */


public class JobParamsInfo {

    /**
     * 执行模式:Standalone,PerJob,Session
     */
    private final String mode;

    private final String userJarPath;

    /**
     * 任务名称
     */
    private final String name;

    /**
     * 提交到Yarn的队列
     */
    private final String queue;

    private final String localPluginRoot; // ??? 暂时不需要

    /**
     * flink配置文件地址
     */
    private final String flinkConfDir;

    private final String flinkJarPath;

    /**
     * yarn配置文件地址
     */
    private final String yarnConfDir;

    private final String pluginLoadMode; // ??? 暂时不需要

    private final String udfJar;

    private final String[] execArgs;

    private final Properties confProperties;

    private final Properties yarnSessionConfProperties;

    /**
     * 依赖的jar包或者其他类型文件
     */
    private final String addShipFile;

    private final Properties dirtyProperties;


    private JobParamsInfo(
            String mode
            , String userJarPath
            , String name
            , String queue
            , String localPluginRoot
            , String flinkConfDir
            , String yarnConfDir
            , String pluginLoadMode
            , String[] execArgs
            , Properties confProperties
            , Properties yarnSessionConfProperties
            , String udfJar
            , String flinkJarPath
            , String addShipFile
            , Properties dirtyProperties) {
        this.mode = mode;
        this.userJarPath = userJarPath;
        this.name = name;
        this.queue = queue;
        this.localPluginRoot = localPluginRoot;
        this.flinkConfDir = flinkConfDir;
        this.yarnConfDir = yarnConfDir;
        this.pluginLoadMode = pluginLoadMode;
        this.execArgs = execArgs;
        this.confProperties = confProperties;
        this.yarnSessionConfProperties = yarnSessionConfProperties;
        this.udfJar = udfJar;
        this.flinkJarPath = flinkJarPath;
        this.addShipFile = addShipFile;
        this.dirtyProperties = dirtyProperties;
    }

    public String getMode() {
        return mode;
    }

    public String getUserJarPath() {
        return userJarPath;
    }

    public String getName() {
        return name;
    }

    public String getQueue() {
        return queue;
    }

    public String getLocalPluginRoot() {
        return localPluginRoot;
    }

    public String getFlinkConfDir() {
        return flinkConfDir;
    }

    public String getFlinkJarPath() {
        return flinkJarPath;
    }

    public String getYarnConfDir() {
        return yarnConfDir;
    }

    public String getPluginLoadMode() {
        return pluginLoadMode;
    }

    public String getUdfJar() {
        return udfJar;
    }

    public String[] getExecArgs() {
        return execArgs;
    }

    public Properties getConfProperties() {
        return confProperties;
    }

    public Properties getYarnSessionConfProperties() {
        return yarnSessionConfProperties;
    }

    public String getAddShipFile() {
        return addShipFile;
    }

    public Properties getDirtyProperties() {
        return dirtyProperties;
    }
}
