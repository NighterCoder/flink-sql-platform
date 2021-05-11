package com.flink.platform.web.utils;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.JobParamsInfo;
import com.flink.platform.web.manager.HDFSManager;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import scala.actors.threadpool.Arrays;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;
import java.util.Properties;

/**
 * Created by 凌战 on 2021/5/11
 */
public class JobGraphBuildUtil {

    private static final String SP = File.separator; // 文件分隔符

    /**
     * 构造JobGraph
     *
     * @param jobParamsInfo 任务构造参数
     * @param hdfsManager   hdfs管理类
     */
    public static JobGraph buildJobGraph(JobParamsInfo jobParamsInfo, HDFSManager hdfsManager) throws Exception {
        // 1.相关配置参数,全局任务并行度等等
        Properties confProperties = jobParamsInfo.getConfProperties();
        int parallelism = Integer.valueOf(confProperties.getProperty(SystemConstants.FLINK_ENV_PARALLELISM, "1"));
        // 2.flink配置文件目录
        String flinkConfDir = jobParamsInfo.getFlinkConfDir();
        // 3.当前用户任务运行参数
        String[] execArgs = jobParamsInfo.getExecArgs();

        // 用户执行jar上传到HDFS的存储路径
        String jarPath = jobParamsInfo.getUserJarPath();
        String dest = "/tmp/" + jobParamsInfo.getName();
        hdfsManager.download(jarPath, dest);
        // 4.构建用户运行 jar File
        File jar = new File(dest);

        // 5.Savepoint设置
        SavepointRestoreSettings savepointRestoreSettings = dealSavepointRestoreSettings(jobParamsInfo.getConfProperties());

        // 6.构建PackagedProgram
        PackagedProgram program = PackagedProgram.newBuilder()
                .setJarFile(jar)
                .setArguments(execArgs)
                .setSavepointRestoreSettings(savepointRestoreSettings)
                .build();

        // 7.flink配置文件
        Configuration flinkConfig = getFlinkConfiguration(flinkConfDir, confProperties);

        // 8.构造JobGraph
        return PackagedProgramUtils.createJobGraph(program, flinkConfig, parallelism, false);

    }


    /**
     * Savepoint配置
     *
     * @param confProperties 配置属性
     * @return SavepointRestoreSettings
     */
    protected static SavepointRestoreSettings dealSavepointRestoreSettings(Properties confProperties) {
        SavepointRestoreSettings savepointRestoreSettings = SavepointRestoreSettings.none();
        String savepointPath = confProperties.getProperty(SystemConstants.SAVEPOINT_PATH_KEY);
        if (StringUtils.isNotBlank(savepointPath)) {
            /**
             * 是否允许跳过无法还原的savepoint,比如删除了原有的部分operator
             */
            String allowNonRestoredState = confProperties.getOrDefault(SystemConstants.ALLOW_NON_RESTORED_STATE_KEY, "false").toString();
            savepointRestoreSettings = SavepointRestoreSettings.forPath(savepointPath, BooleanUtils.toBoolean(allowNonRestoredState));
        }
        return savepointRestoreSettings;
    }


    public static Configuration getFlinkConfiguration(String flinkConfDir, Properties confProperties) {
        Configuration flinkConfig = StringUtils.isEmpty(flinkConfDir) ? new Configuration() : GlobalConfiguration.loadConfiguration(flinkConfDir);
        confProperties.forEach((key, val) -> flinkConfig.setString(key.toString(), val.toString()));
        return flinkConfig;
    }


    public static void fillCustomJarForJobGraph(String jarPath, JobGraph jobGraph) throws UnsupportedEncodingException {
        String addJarPath = URLDecoder.decode(jarPath, Charsets.UTF_8.toString());
        if (addJarPath.length()>2){
            addJarPath = addJarPath.substring(1,addJarPath.length()-1).replace("\"","");
        }

        List<String> paths = Arrays.asList(addJarPath.split(","));
        paths.forEach(path -> jobGraph.addJar(new Path("file://" + path)));
    }



}
