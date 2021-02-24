package com.flink.platform.web.common.entity;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Column {
    private String title;
    private String dataIndex;

    public Column(String title) {
        this.title = title;
        this.dataIndex = title;
    }

}
