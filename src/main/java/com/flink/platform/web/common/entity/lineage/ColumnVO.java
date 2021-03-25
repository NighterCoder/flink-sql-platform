package com.flink.platform.web.common.entity.lineage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by 凌战 on 2021/3/24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnVO {

    /**
     * 字段名
     */
    private String col;
    /**
     * 库名.表名.字段名
     */
    private String fullCol;
    /**
     * 当前列所属的表
     */
    private TableLineageInputOutput table;
    /**
     * 字段类型
     */
    private String type;

}
