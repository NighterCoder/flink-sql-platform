package com.flink.platform.web.common.util;


import freemarker.cache.StringTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * SQL工具处理类
 * Created by 凌战 on 2021/2/20
 */
@Slf4j
public class SQLUtils {

    /**
     * 格式化SQL,去除每一行注释
     * <p>
     * 以换行符为分割符,如果
     *
     * @param sql
     */
    public static String commentsFormat(String sql) {

        List<String> lines = Arrays.stream(sql.split("\n"))
                .map(s -> {
                    int index = s.indexOf("--");
                    if (index > -1) {
                        return s.substring(0, index);
                    }
                    return s;
                }).collect(Collectors.toList());

        String formatted = StringUtils.join(lines, "\n");

        log.debug("Comments format :" + formatted);

        return formatted;
    }


    /**
     * 基于Freemarker解析SQL
     *
     * @param sql    sql
     * @param params params
     */
    public static String generateSQL(String sql, Map<String, Object> params) {
        String name = UUID.randomUUID().toString();
        try {
            Configuration conf = new Configuration(freemarker.template.Configuration.VERSION_2_3_27);
            conf.setDefaultEncoding("UTF-8");
            conf.setTemplateExceptionHandler(TemplateExceptionHandler.DEBUG_HANDLER);
            conf.setLogTemplateExceptions(true);
            conf.setWrapUncheckedExceptions(true);
            StringTemplateLoader templateLoader = new StringTemplateLoader();
            templateLoader.putTemplate(name, sql);
            conf.setTemplateLoader(templateLoader);

            Template template = conf.getTemplate(name);
            Writer out = new StringWriter();
            template.process(params, out);
            sql = out.toString();
            out.close();
        } catch (IOException | TemplateException e) {
            log.error("生成 SQL 语句错误：", e);
            throw new RuntimeException(e);
        }

        return sql;
    }


}
