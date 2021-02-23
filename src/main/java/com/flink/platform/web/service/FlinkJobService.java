package com.flink.platform.web.service;

import com.flink.platform.core.exception.SqlPlatformException;
import com.flink.platform.core.executor.PlatformAbstractJobClusterExecutor;
import com.flink.platform.web.common.entity.FetchData;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.jar.JarConf;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.enums.StatementState;
import com.flink.platform.web.common.param.FlinkSessionCreateParam;
import com.flink.platform.web.manager.FlinkSessionManager;
import com.flink.platform.web.manager.HDFSManager;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptions;
//import org.apache.flink.core.plugin.TemporaryClassLoaderContext;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.net.URL;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;

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
    public String submitJar(JarConf jarConf) throws Exception {
        // 先去下载jar包
        String jarPath=jarConf.getJarPath();
        String dest = "c:/tmp/"+jarConf.getJarName();
        hdfsManager.download(jarPath,dest);
        // 构建jar File
        File jar = new File(dest);

        List<URL> userClassPaths = new ArrayList<>();
        File file = ResourceUtils.getFile(new URL(Objects.requireNonNull(this.getClass().getClassLoader().getResource("")).toString()+"lib"));
        if(file.isDirectory()&&file.listFiles()!=null){
            for(File ele: Objects.requireNonNull(file.listFiles())) {
                userClassPaths.add(ele.toURI().toURL());
            }
        }

        // 构建PackagedProgram
        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder()
                .setJarFile(jar)
                .setUserClassPaths(userClassPaths)
                .build();

        // 获取Configuration
        String configurationDirectory = CliFrontend.getConfigurationDirectoryFromEnv();

        // 2. load the global configuration
        // 加载 flink-conf.yaml构成 Configuration
        Configuration configuration = GlobalConfiguration.loadConfiguration(configurationDirectory);


        // 3. 加载jar包
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.JARS,
                packagedProgram.getJobJarAndDependencies(),
                URL::toString
        );

        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.CLASSPATHS,
                packagedProgram.getClasspaths(),
                URL::toString
        );


        Pipeline pipeline = this.wrapClassLoader(packagedProgram.getUserCodeClassLoader(),() -> {
            try {
                return PackagedProgramUtils.
                        getPipelineFromProgram(packagedProgram,
                                configuration,
                                10,
                                false);
            } catch (ProgramInvocationException e) {
                e.printStackTrace();
                return null;
            }
        });


        // yarn-per-job模式
        return new PlatformAbstractJobClusterExecutor<>(new YarnClusterClientFactory()).
                execute(pipeline,configuration,packagedProgram.getUserCodeClassLoader()).get().getJobID().toString();

    }


    public <R> R wrapClassLoader(ClassLoader classLoader,Supplier<R> supplier) {
        try (TemporaryClassLoaderContext tmpCl = TemporaryClassLoaderContext.of(classLoader)) {
            return supplier.get();
        }
    }



}
