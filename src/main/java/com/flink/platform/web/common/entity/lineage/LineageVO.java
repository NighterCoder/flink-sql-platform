package com.flink.platform.web.common.entity.lineage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

/**
 * Created by 凌战 on 2021/3/23
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LineageVO {

    private String tableName;

    private List<Map<String,String>>  columnInfo;

}
