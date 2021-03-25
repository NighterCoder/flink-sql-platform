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
public class ColumnLineage {

    private ColumnLineageInputOutput inputOutput;

}
