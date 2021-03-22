package com.flink.platform.web.common.enums;

/**
 *
 * livy会返回的state列举
 * https://livy.incubator.apache.org/docs/latest/rest-api.html#session
 *
 * Created by 凌战 on 2021/3/22
 */
public enum SparkSessionState {
    NOT_STARTED("not_started",SessionState.PREP),
    STARTING("starting",SessionState.PREP),
    IDLE("idle",SessionState.RUNNING),
    RUNNING("running",SessionState.RUNNING),
    BUSY("busy",SessionState.RUNNING),
    SHUTTING_DOWN("shutting_down",SessionState.NONE),
    ERROR("error", SessionState.NONE),
    DEAD("dead", SessionState.NONE),
    KILLED("killed", SessionState.NONE),
    SUCCESS("success", SessionState.NONE),
    ;


    private String livyState;
    private SessionState sessionState;


    SparkSessionState(String livyState, SessionState sessionState){
        this.livyState = livyState;
        this.sessionState = sessionState;
    }

    /**
     * 将Livy的state转换成统一的State
     * @param livyState livy state
     */
    public static SessionState stateOf(String livyState){
        for (SparkSessionState sessionState:SparkSessionState.values()){
            if (sessionState.name().equals(livyState)){
                return sessionState.sessionState;
            }
        }
        return SessionState.NONE;
    }



}
