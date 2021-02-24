package com.flink.platform.web.common.enums;

public enum SessionType {
    FLINK(1, "flink"),
    SPARK(2, "spark");

    private Integer code;
    private String name;

    SessionType(Integer code, String name) {
        this.code = code;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public Integer getCode() {
        return code;
    }

    public static SessionType fromCode(Integer code) {
        for (SessionType type : SessionType.values()) {
            if (type.code == code) {
                return type;
            }
        }
        return SPARK;
    }

}
