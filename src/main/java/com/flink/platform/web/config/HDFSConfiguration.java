package com.flink.platform.web.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;


/**
 * Created by 凌战 on 2021/2/22
 */
@Configuration
public class HDFSConfiguration {

    @Value("${hdfs.path}")
    private String path;

    @Bean
    public org.apache.hadoop.conf.Configuration getConfiguration() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();

        conf.addResource(String.join(File.separator, path, "core-site.xml"));
        conf.addResource(String.join(File.separator, path, "hdfs-site.xml"));
        conf.addResource(String.join(File.separator, path, "hive-site.xml"));

        return conf;
    }




}
