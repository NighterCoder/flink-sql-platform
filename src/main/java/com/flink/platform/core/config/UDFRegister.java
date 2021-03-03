package com.flink.platform.core.config;

/**
 * Created by 凌战 on 2021/3/3
 */
public class UDFRegister {
    private String functionName;
    private String className;
    private String jarName;

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getJarName() {
        return jarName;
    }

    public void setJarName(String jarName) {
        this.jarName = jarName;
    }
}
