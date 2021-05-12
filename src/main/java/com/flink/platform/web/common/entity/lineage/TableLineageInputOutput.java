package com.flink.platform.web.common.entity.lineage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Map;

/**
 * Created by 凌战 on 2021/3/24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableLineageInputOutput {

    private String db;
    private String table;
    private Integer ownerId;
    private String dbType;
    private Map<String,String> props;
    private Timestamp createAt;

}
