package com.flink.platform.web.common.entity;

import lombok.Builder;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 执行SQL获取结果
 */
@Data
public class FetchData {
    /**
     * streaming or batch
     */
    private String executeType;
    private String sessionId;
    private String jobId;
    /**
     * 任务状态
     */
    private String jobStatus;
    /**
     * yarn applicationId
     */
    private String applicationId;

    @Builder.Default
    private List<Column> columns=new ArrayList<>();
    @Builder.Default
    private List<Map<String, Object>> rows = new ArrayList<>();

    /**
     * 添加返回结果列
     * @param title 列名
     */
    public void addColumn(String title){
        this.columns.add(new Column(title,title));
    }

    /**
     * 构造结果
     * @param index 行序号
     * @param key 对应列
     * @param value 对应值
     */
    public void addRow(int index,String key,Object value){
        if(rows.size()==index){
            rows.add(new HashMap<>());
        }
        rows.get(index).put(key, value);
    }


    public FetchData() {
    }

    public FetchData(String sessionId, String jobId) {
        this.sessionId = sessionId;
        this.jobId = jobId;
    }

}
