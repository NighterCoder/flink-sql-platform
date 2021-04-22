package com.flink.platform.web.common.entity.entity2table;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * Created by 凌战 on 2021/3/1
 */

@TableName(value = "function")
@Data
@Accessors(chain = true)
public class FunctionDO implements Serializable {

    private Long id;

    private String functionName;

    private String className;

    private String jars;

    private String username;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "Asia/Shanghai")
    private Timestamp createTime;

}
