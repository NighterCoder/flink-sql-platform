package com.flink.platform.web.common.entity.lineage;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Created by 凌战 on 2021/5/13
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class SelectRel {

    private List<String> columns;
    private String fromTables;
    private List<SelectRel> childSelect;

}
