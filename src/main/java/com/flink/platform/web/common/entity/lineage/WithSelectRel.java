package com.flink.platform.web.common.entity.lineage;

import lombok.*;

/**
 * Created by 凌战 on 2021/5/12
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class WithSelectRel {

    private String withName;
    private SelectRel selectRel;

}
