package com.flink.platform.web.service;

import com.flink.platform.core.context.ExecutionContext;
import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.executor.PlatformAbstractJobClusterExecutor;
import com.flink.platform.core.executor.PlatformYarnJobClusterExecutor;
import com.flink.platform.web.common.entity.FetchData;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.jar.JarConf;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.enums.StatementState;
import com.flink.platform.web.manager.FlinkSessionManager;
import com.flink.platform.core.rest.session.Session;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.manager.HDFSManager;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.Collections;
import java.util.Map;

@Service
public class FlinkJobService {

    @Autowired
    private FlinkSessionManager sessionManager;

    @Autowired
    private HDFSManager hdfsManager;


    /**
     * 创建Session
     * @param param 创建Session的可选参数
     */
    public String createSession(FlinkSessionCreateParam param) {
        String sessionName = param.getSessionName();
        String planner = param.getPlanner();
        String executionType = param.getExecutionType();
        Map<String, String> properties = param.getProperties();
        if (properties == null) {
            properties = Collections.emptyMap();
        }
        String sessionId;
        try {
            sessionId = sessionManager.createSession(sessionName, planner, executionType, properties);
        } catch (Exception e) {
            throw new SqlPlatformException(e.getMessage());
        }
        return sessionId;
    }

    /**
     * 查询指定Session的状态
     * @param sessionId SessionId
     */
    public SessionState sessionHeartBeat(String sessionId){
        return sessionManager.statusSession(sessionId);
    }

    /**
     * 返回SQL执行的结果
     * @param sql 执行SQL
     * @param sessionId sessionId
     * @return StatementResult
     */
    public StatementResult submit(String sql,String sessionId){
        StatementResult result = new StatementResult();
        result.setStatement(sql);
        // jobId is not null only after job is submitted
        FetchData fetchData = sessionManager.submit(sql,sessionId);
        if(fetchData.getJobId()!=null){
            // JobId依旧存在,还在执行当中
            result.setJobId(fetchData.getJobId());
            result.setState(StatementState.RUNNING);
        }else{
            //JobId不存在
            result.setState(StatementState.DONE);
            result.setColumns(fetchData.getColumns());
            result.setRows(fetchData.getRows());
        }
        result.setEnd(System.currentTimeMillis());

        return result;
    }

    /**
     * 提交Jar包
     * @param jarConf 参数类
     */
    public void submitJar(JarConf jarConf) throws Exception {
        // 先去下载jar包
        String jarPath=jarConf.getJarPath();
        String dest = "/tmp/"+jarConf.getJarName();
        hdfsManager.download(jarPath,dest);
        // 构建jar File
        File jar = new File(dest);

        // 构建PackagedProgram
        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder()
                .setJarFile(jar)
                .setEntryPointClassName(jarConf.getEntryClass())
                .build();
        // 构建Pipeline
        Pipeline pipeline = PackagedProgramUtils.
                getPipelineFromProgram(packagedProgram,
                        new Configuration(),
                        10,
                        false);

        // yarn-per-job模式
        // todo 需要找到Configuration
        new PlatformAbstractJobClusterExecutor<>(new YarnClusterClientFactory()).
                execute(pipeline,new Configuration(),null);

    }


}
