package com.flink.platform.web.common.entity.lineage;

import lombok.*;

import java.util.List;

/**
 * Created by 凌战 on 2021/5/12
 */
@Data
@Builder
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class CreateTableRel {

    private String scheduleId;
    private String scheduleTopologyNodeId;
    private String scheduleSnapshotId; // 在schedule发生改变时需要判定血缘关系是不是要改变

    private String table;
    private List<ColumnVO> columns;

}
