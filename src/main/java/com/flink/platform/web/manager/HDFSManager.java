package com.flink.platform.web.manager;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * HDFS Manager: 文件上传,下载,删除
 *
 * Created by 凌战 on 2021/2/22
 */
@Component
public class HDFSManager {

    @Autowired
    private Configuration conf;

    public void write(String path, InputStream input) throws Exception {
        try (FileSystem fs = FileSystem.get(this.conf)) {
            FSDataOutputStream output = fs.create(new Path(path), true);
            IOUtils.copyLarge(input, output);
        }
    }

    public void delete(String path) throws Exception {
        try (FileSystem fs = FileSystem.get(this.conf)) {
            fs.delete(new Path(path), true);
        }
    }

    public void download(String source, String sink) throws Exception {
        try (FileSystem fs = FileSystem.get(this.conf)) {
            FSDataInputStream in = fs.open(new Path(source));//获取文件流
            FileUtils.copyInputStreamToFile(in, new File(sink));
        }
    }

}
