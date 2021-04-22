package com.flink.platform.web.service.impl;

import com.flink.platform.web.common.entity.entity2table.FunctionDO;
import com.flink.platform.web.common.entity.login.LoginUser;
import com.flink.platform.web.manager.HDFSManager;
import com.flink.platform.web.mapper.FunctionMapper;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 凌战 on 2021/3/1
 */
@Service
public class FunctionService {


    @Autowired
    private FunctionMapper functionMapper;

    @Autowired
    private HDFSManager hdfsManager;

    @Value("${hdfs.upload}")
    private String uploadPath;

    /**
     * @param functionName 自定义方法名称,即在flink sql中可以使用的自定义函数名
     * @param className    主类名
     * @param files        自定义文件
     */
    public void save(String functionName,
                     String className,
                     MultipartFile[] files) throws Exception {

        LoginUser loginUser= (LoginUser) SecurityContextHolder.getContext().getAuthentication() .getPrincipal();
        String username = loginUser.getUsername();
        FunctionDO functionDO = new FunctionDO();
        functionDO.setUsername(username);
        functionDO.setClassName(className);
        functionDO.setFunctionName(functionName);
        functionDO.setJars(upload(files));

        functionMapper.insert(functionDO);
    }


    /**
     * 上传的jar包命名方式: timestamp + original name
     * <p>
     * 自定义函数文件
     *
     * @param files 自定义文件
     */
    private String upload(MultipartFile[] files) throws Exception {
        List<String> jars = new ArrayList<>();
        for (MultipartFile file : files) {
            // path路径
            // File.separator这个代表系统目录中的间隔符,说白了就是斜线,不过有时候需要双线,有时候是单线,你用这个静态变量就解决兼容问题了
            String path = String.join(
                    File.separator,
                    uploadPath,
                    System.currentTimeMillis() + "-" + file.getOriginalFilename()
            );
            jars.add(path);
            hdfsManager.write(path, file.getInputStream());
        }
        return StringUtils.join(jars, ";");
    }

}
