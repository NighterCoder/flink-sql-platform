package com.flink.platform.web.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class DtoCluster extends AbstractPageDto {

    private Integer id;
    private String name;
    private String yarnUrl;
    /**
     * fileSystem默认的端口号,在core-site.xml中配置,也有可能是8020端口
     * hdfs://node1:9000
     */
    private String fsDefaultFs;
    /**
     * 在dfs.webhdfs.enabled属性设置为true
     * 一般访问namenode的ip和50070端口
     * webhdfs://namenode1:50070
     */
    private String fsWebhdfs;
    private String fsUser;
    private String fsDir;
    private Boolean defaultFileCluster;
    private Boolean flinkProxyUserEnabled;
    private String streamBlackNodeList;
    private String batchBlackNodeList;


    @Override
    public String validate() {
        if (StringUtils.isBlank(name)){
            return "名称不能为空";
        }
        if (StringUtils.isBlank(yarnUrl)){
            return "yarn管理地址不能为空";
        }
        if (StringUtils.isBlank(fsDefaultFs)){
            return "fs.DefaultFS不能为空";
        }
        if (StringUtils.isBlank(fsWebhdfs)){
            return "fs.webhdfs不能为空";
        }
        if (StringUtils.isBlank(fsUser)){
            return "操作用户不能为空";
        }
        if (StringUtils.isBlank(fsDir)){
            return "程序包存储目录不能为空";
        }
        if ("/".equals(fsDir)){
            return "不能使用根目录作为存储目录";
        }

        // 处理存储路径
        if (!fsDir.startsWith("/")){
            fsDir = "/"+fsDir;
        }
        if (fsDir.endsWith("/")){
            fsDir = fsDir.substring(0, fsDir.length() - 1);
        }
        return null;
    }
}
