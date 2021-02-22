package com.flink.platform.web.common.entity.analysis;

import com.baomidou.mybatisplus.annotation.TableLogic;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * Created by 凌战 on 2021/2/20
 */

@TableName(value = "session")
@Data
@Accessors(chain = true)
public class SessionDO implements Serializable {

    private Long id;

    private String username;

    private String sessionId;

    private Integer sessionType;

    private Integer executeType;

    @TableLogic
    private Integer deleted;

}
