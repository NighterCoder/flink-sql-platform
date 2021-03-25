package com.flink.platform.web.common.entity.lineage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * Created by 凌战 on 2021/3/24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableLineageInputOutput {

    private String db;
    private String table;
    private String owner;
    private String dbType;
    private Timestamp createAt;

}
