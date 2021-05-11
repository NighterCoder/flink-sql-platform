package com.flink.platform.core.executor.v2.executor;

import com.flink.platform.web.common.entity.JobParamsInfo;
import com.flink.platform.web.manager.HDFSManager;
import com.flink.platform.web.utils.JobGraphBuildUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnConfigOptionsInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Optional;

/**
 * Created by 凌战 on 2021/5/11
 */
public class YarnJobClusterExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(YarnJobClusterExecutor.class);

    // 日志文件,优先加载log4j.properties,不存在才会加载logback.xml
    private static final String CONFIG_FILE_LOGBACK_NAME = "logback.xml";
    private static final String CONFIG_FILE_LOG4J_NAME = "log4j.properties";

    private static final String DEFAULT_TOTAL_PROCESS_MEMORY = "1024m";


    JobParamsInfo jobParamsInfo;
    HDFSManager hdfsManager;


    public YarnJobClusterExecutor(JobParamsInfo jobParamsInfo, HDFSManager hdfsManager) {
        this.jobParamsInfo = jobParamsInfo;
        this.hdfsManager = hdfsManager;
    }

    public void exec() throws Exception {
        JobGraph jobGraph = JobGraphBuildUtil.buildJobGraph(jobParamsInfo, hdfsManager);
        /**
         * 添加UDF Jar包
         */
        if (!StringUtils.isBlank(jobParamsInfo.getUdfJar())) {
            JobGraphBuildUtil.fillCustomJarForJobGraph(jobParamsInfo.getUdfJar(), jobGraph);
        }

        Configuration flinkConfiguration = JobGraphBuildUtil.getFlinkConfiguration(jobParamsInfo.getFlinkConfDir(), jobParamsInfo.getConfProperties());
        appendApplicationConfig(flinkConfiguration,jobParamsInfo);


        //YarnClusterClientFactory



    }


    /**
     * 追加Flink Config
     *
     * @param flinkConfig   flink 配置文件
     * @param jobParamsInfo 任务配置类
     */
    private void appendApplicationConfig(Configuration flinkConfig, JobParamsInfo jobParamsInfo) {
        if (!StringUtils.isEmpty(jobParamsInfo.getName())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_NAME, jobParamsInfo.getName());
        }
        if (!StringUtils.isEmpty(jobParamsInfo.getQueue())) {
            flinkConfig.setString(YarnConfigOptions.APPLICATION_QUEUE, jobParamsInfo.getQueue());
        }
        if (!StringUtils.isEmpty(jobParamsInfo.getFlinkConfDir())) {
            discoverLogConfigFile(jobParamsInfo.getFlinkConfDir()).ifPresent(file ->
                    flinkConfig.setString(YarnConfigOptionsInternal.APPLICATION_LOG_CONFIG_FILE, file.getPath()));
        }
        if (!flinkConfig.contains(TaskManagerOptions.TOTAL_PROCESS_MEMORY)) {
            flinkConfig.setString(TaskManagerOptions.TOTAL_PROCESS_MEMORY.key(), DEFAULT_TOTAL_PROCESS_MEMORY);
        }
    }


    /**
     * 根据flink-conf目录查找日志配置文件
     *
     * @param configurationDirectory Flink配置文件目录
     */
    private Optional<File> discoverLogConfigFile(String configurationDirectory) {
        Optional<File> logConfigFile = Optional.empty();
        final File log4jFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOG4J_NAME);
        // 先去查找log4j.properties
        if (log4jFile.exists()) {
            logConfigFile = Optional.of(log4jFile);
        }
        final File logbackFile = new File(configurationDirectory + File.separator + CONFIG_FILE_LOGBACK_NAME);
        if (logbackFile.exists()) {
            if (logConfigFile.isPresent()) {
                LOG.warn("The configuration directory ('" + configurationDirectory + "') already contains a LOG4J config file." +
                        "If you want to use logback, then please delete or rename the log configuration file.");
            } else {
                logConfigFile = Optional.of(logbackFile);
            }
        }
        return logConfigFile;
    }


}
