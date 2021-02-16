package com.flink.platform.web.common.util;

import lombok.Data;

/**
 * 分页实体类
 */
@Data
public class Pages {

    /**
     * 当前页码
     */
    private int pageNum;
    /**
     * 每页数量
     */
    private int pageSize;
    /**
     * 记录总数
     */
    private long totalSize;
    /**
     * 页码总数
     */
    private int totalPage;


}
