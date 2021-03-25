package com.flink.platform.web.common.entity.lineage;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Created by 凌战 on 2021/3/24
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ColumnLineageInputOutput {
    private List<ColumnLineageInputOutputEle> input;
    private ColumnLineageInputOutputEle output;
}
