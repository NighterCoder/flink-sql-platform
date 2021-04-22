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

    String SINK_DRIVER = "sink.driver";
    String SINK_URL = "sink.url";
    String SINK_TABLE = "sink.table";


    /**
     * 调度任务可视化时间维度
     */
    int TIMER_CYCLE_MINUTE = 1; // 分钟维度执行,比如间隔几分钟执行,选择该维度需要有间隔时间
    int TIMER_CYCLE_HOUR = 2;   // 小时维度执行,需要选择在每小时的第几分钟执行
    int TIMER_CYCLE_DAY = 3;    // 天维度执行,在每天的第几小时第几分钟执行
    int TIMER_CYCLE_WEEK = 4;   // 周维度执行,

    /**
     * 节点类型
     * todo 日后可以补充..
     */
    interface NodeType {
        String SPARK_BATCH_JAR = "spark_batch_jar";
        String SPARK_BATCH_SQL = "spark_batch_sql";


        String FLINK_STREAM_JAR = "flink_stream_jar";
        String FLINK_STREAM_SQL = "flink_stream_sql";
        String FLINK_BATCH_SQL = "flink_batch_sql";
    }


    /**
     * Job分组
     */
    interface JobGroup {
        String COMMON = "common";
        String MONITOR = "monitor";
        String SCHEDULE = "schedule";
        String SCRIPT_HISTORY = "scriptHistory";
    }

    /**
     * 执行状态
     */
    interface JobState {
        String INITED = "INITED";
        String SUBMITTING = "SUBMITTING";
        String SUBMITTED = "SUBMITTED";
        String ACCEPTED = "ACCEPTED";
        String RUNNING = "RUNNING";
        String SUCCEEDED = "SUCCEEDED";
        String KILLED = "KILLED";
        String FAILED = "FAILED";
        String TIMEOUT = "TIMEOUT";
        String SUBMITTING_TIMEOUT = "SUBMITTING_TIMEOUT";
        String SUBMITTING_FAILED = "SUBMITTING_FAILED";
        /**
         * 调度扩展执行状态
         */
        String UN_CONFIRMED_ = "UN_CONFIRMED";
        String WAITING_PARENT_ = "WAITING_PARENT";
        String PARENT_FAILED_ = "PARENT_FAILED";
    }


    /**
     * 钉钉机器人消息API
     */
    String DINGDING_ROBOT_URL = "https://oapi.dingtalk.com/robot/send?access_token=";


}
