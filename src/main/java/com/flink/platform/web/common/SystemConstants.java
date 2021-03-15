package com.flink.platform.web.common;

import com.flink.platform.core.config.entries.ExecutionEntry;

import java.util.Arrays;
import java.util.List;

public interface SystemConstants {

    // 可用的FLINK执行PLANNER
    List<String> AVAILABLE_EXECUTION_PLANNERS = Arrays.asList(
            ExecutionEntry.EXECUTION_PLANNER_VALUE_OLD,
            ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);

    // 可用的FLINK执行TYPE
    List<String> AVAILABLE_EXECUTION_TYPES = Arrays.asList(
            ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH,
            ExecutionEntry.EXECUTION_TYPE_VALUE_STREAMING);

    // FLINK的lib jar包目录
    String FLINK_LIB_DIR = "lib";

    String SINK_DRIVER="sink.driver";
    String SINK_URL="sink.url";
    String SINK_TABLE="sink.table";



}
