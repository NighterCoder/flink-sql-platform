package com.flink.platform.web.common.entity.jar;

import lombok.Data;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
public class JarJobConf {
    private Long id;
    private String userJarPath; // 用户Flink Jar包地址
    private String userJarName; // 用户Flink Jar包名称
    private List<String> userClassPaths = Collections.emptyList(); // 用户上传依赖Jar包地址

    /**
     * 自定义任务名称
     */
    private String name; // 自定义任务名称 (Yarn模式下就是yarn name)

    /**
     * 主类名称
     */
    private String entryClass; // Flink Jar方法类全路径名称

    /**
     * Savepoint保存路径
     */
    private String savepointPath = ""; // savepoint路径

    /**
     * Flink运行参数
     */
    private String[] args = new String[0];

    /**
     * Application ID
     */
    private String applicationId = "";

    /**
     * JobGraph生成的JobID
     */
    private String jobId;

    public List<URL> parseUserClassPaths() throws MalformedURLException {
        if (CollectionUtils.isNotEmpty(this.userClassPaths)) {
            List<URL> userClassPaths = new ArrayList<>();
            for (String ele : this.userClassPaths) {
                userClassPaths.add(new URL(ele));
            }
            return userClassPaths;
        }
        return Collections.emptyList();
    }


    public SavepointRestoreSettings parseSavepointRestoreSettings(){
        if (StringUtils.isNotBlank(this.savepointPath)) {
            return SavepointRestoreSettings.forPath(this.savepointPath);
        }
        return SavepointRestoreSettings.none();
    }

    // 将JobID Str转换成JobID
    public JobID parseJobId(){
        if (StringUtils.isNotBlank(this.jobId)){
            return JobID.fromHexString(this.jobId);
        }
        return null;
    }

}
