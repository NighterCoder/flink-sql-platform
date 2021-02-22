package com.flink.platform.web.service;

import com.flink.platform.web.common.entity.jar.JarDTO;
import com.flink.platform.web.manager.HDFSManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import com.flink.platform.web.common.util.DateUtils;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;


/**
 * Created by 凌战 on 2021/2/22
 */
@Service
@Slf4j
public class JarManagerService {


    @Autowired
    private HDFSManager hdfsManager;


    @Value("${flink.upload.path}")
    private String flinkRootPath;


    /**
     * 上传jar包文件
     * @link https://blog.csdn.net/qq_36314960/article/details/104775557
     * @param file jar文件
     */
    public void upload(MultipartFile file){
        // 获取文件完整名[文件名+扩展名]
        String jarName=file.getOriginalFilename();
        // todo 获取username
        String username = "test";

        JarDTO jarDTO = new JarDTO();
        jarDTO.setJarName(jarName);
        jarDTO.setJarPath(generateFileUploadPath(jarName,username));
        jarDTO.setUsername(username);
        jarDTO.setUploadTime(Timestamp.valueOf(LocalDateTime.now()));

    }


    /**
     * 构造jar包上传路径 username_time_jar文件
     * hdfs://基础路径/username_time_jar文件
     * @param jarName jar包名称
     * @param username 用户名
     */
    private String generateFileUploadPath(String jarName,String username){
        String currentTime = DateUtils.format(LocalDate.now(),"yyyyMMdd_HHmmss");
        return String.format("hdfs://%s/%s_%s_%s",flinkRootPath,username,currentTime,jarName);
    }



}
