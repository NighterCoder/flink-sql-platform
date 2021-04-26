package com.flink.platform.web.common.entity;

/**
 * 背压信息
 *
 * Created by 凌战 on 2021/4/26
 */
public class BackpressureInfo {

    /**
     * 实际ratio * 100,方便监控任务处理
     */
    public final int ratio;
    public final String nextVertex;

    public BackpressureInfo(int ratio, String nextVertex) {
        this.ratio = ratio;
        this.nextVertex = nextVertex;
    }

}
