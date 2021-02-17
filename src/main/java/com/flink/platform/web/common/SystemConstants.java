package com.flink.platform.web.common;

import com.flink.platform.core.config.entries.ExecutionEntry;

import java.util.Arrays;
import java.util.List;

public interface SystemConstants {

     List<String> AVAILABLE_PLANNERS = Arrays.asList(
            ExecutionEntry.EXECUTION_PLANNER_VALUE_OLD,
            ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);

    List<String> AVAILABLE_EXECUTION_TYPES = Arrays.asList(
            ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH,
            ExecutionEntry.EXECUTION_TYPE_VALUE_STREAMING);


}
