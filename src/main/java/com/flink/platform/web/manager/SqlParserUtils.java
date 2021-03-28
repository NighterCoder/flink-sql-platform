package com.flink.platform.web.manager;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

/**
 * Created by 凌战 on 2021/3/26
 */
public class SqlParserUtils {

    /**
     * 获取默认的Flink SQL语义
     *
     * @return SqlParser
     */
    public static SqlParser getFlinkSqlParser(String statement) {
        return SqlParser.create(statement, SqlParser.configBuilder()
                // 使用FlinkSqlParse工厂
                .setParserFactory(FlinkSqlParserImpl.FACTORY)
                .setQuoting(BACK_TICK)
                .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
                .setQuotedCasing(Casing.UNCHANGED)
                .setConformance(FlinkSqlConformance.DEFAULT)
                .build());
    }


    /**
     * 获取切换为Hive语义的SqlParser
     *
     * @return SqlParser
     */
    public static SqlParser getFlinkHiveSqlParser(String statement) {
        return SqlParser.create(statement, SqlParser.configBuilder()
                // 使用FlinkSqlParse工厂
                .setParserFactory(FlinkHiveSqlParserImpl.FACTORY)
                .setQuoting(BACK_TICK)
                .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
                .setQuotedCasing(Casing.UNCHANGED)
                .setConformance(FlinkSqlConformance.HIVE)
                .build());
    }

}
