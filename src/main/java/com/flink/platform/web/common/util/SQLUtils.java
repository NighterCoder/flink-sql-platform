package com.flink.platform.web.common.util;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL工具处理类
 * Created by 凌战 on 2021/2/20
 */
@Slf4j
public class SQLUtils {

    /**
     * 格式化SQL,去除每一行注释
     *
     * 以换行符为分割符,如果
     *
     * @param sql
     */
    public static String commentsFormat(String sql){

        List<String> lines= Arrays.stream(sql.split("\n"))
                .map(s->{
                    int index = s.indexOf("--");
                    if(index > -1){
                        return s.substring(0,index);
                    }
                    return s;
                }).collect(Collectors.toList());

        String formatted = StringUtils.join(lines,"\n");

        log.debug("Comments format :" + formatted);

        return formatted;
    }


    /**
     * 基于Freemarker解析SQL
     *
     * @param sql sql
     * @param params params
     */
    public static String generateSQL(String sql, Map<String,Object> params){

    }





}
