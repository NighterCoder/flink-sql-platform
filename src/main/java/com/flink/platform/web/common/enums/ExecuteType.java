package com.flink.platform.web.common.enums;

public enum ExecuteType {

    BATCH(0,"batch"),
    STREAMING(1,"streaming");


    private Integer code;
    private String type;

    ExecuteType(Integer code, String type) {
        this.code = code;
        this.type = type;
    }

    public static ExecuteType fromCode(Integer code){
        for (ExecuteType typeEnum : ExecuteType.values()) {
            if (typeEnum.code.equals(code)) {
                return typeEnum;
            }
        }
        return BATCH;
    }

    public String getType() {
        return this.type;
    }
    public Integer getCode() {
        return this.code;
    }
}
