package com.flink.platform.web.manager;

import com.flink.platform.web.common.entity.lineage.SelectRel;
import com.flink.platform.web.exception.FlinkSqlParseException;
import com.flink.platform.web.service.impl.FlinkLineageAnalysisUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.hive.impl.FlinkHiveSqlParserImpl;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

/**
 * Created by 凌战 on 2021/3/23
 */
@Slf4j
public class FlinkSqlParseTest {


    public static List<String> parseFlinkSql(String sql) {
        List<String> sqlList = new ArrayList<>();
        if (sql != null && !sql.isEmpty()) {
            try {
//                SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
//                        //.setParserFactory(FlinkSqlParserImpl.FACTORY) // 改成Hive
//                        .setParserFactory(FlinkHiveSqlParserImpl.FACTORY)
//                        .setQuoting(BACK_TICK)
//                        .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
//                        .setQuotedCasing(Casing.UNCHANGED)
//                        .setConformance(FlinkSqlConformance.HIVE) // 可以改成hive语义
//                        .build()
//                );

                SqlParser parser = SqlParser.create(sql, SqlParser.configBuilder()
                        // 使用FlinkSqlParse工厂
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setQuoting(BACK_TICK)
                        .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setConformance(FlinkSqlConformance.DEFAULT)
                        .build());

                List<SqlNode> sqlNodeList = parser.parseStmtList().getList();


                SelectRel selectRel = new SelectRel();
                FlinkLineageAnalysisUtils.parseSelectNode(sqlNodeList.get(0),selectRel);
                System.out.println(selectRel);


 /*               if (sqlNodeList != null && !sqlNodeList.isEmpty()) {
                    for (SqlNode sqlNode : sqlNodeList) {
                        if (sqlNode instanceof SqlCreateTable) {
                            // 创建的表名
                            String tableName = ((SqlCreateTable) sqlNode).getTableName().toString();
                            // 当前表的字段列表
                            // SqlNode -> SqlTableColumn -> (SqlRegularColumn,SqlComputedColumn)
                            List<Map<String, String>> columnInfo = ((SqlCreateTable) sqlNode).getColumnList().getList().stream().map(s -> {

                                Map<String, String> columnMap = new HashMap<>();

                                if (s instanceof SqlTableColumn.SqlRegularColumn) {
                                    String columnName = ((SqlTableColumn.SqlRegularColumn) s).getName().toString();
                                    String columnType = ((SqlTableColumn.SqlRegularColumn) s).getType().getTypeNameSpec().getTypeName().toString();
                                    columnMap.put(columnName, columnType);

                                } else if (s instanceof SqlTableColumn.SqlComputedColumn) {
                                    String columnName = ((SqlTableColumn.SqlComputedColumn) s).getName().toString();
                                    String exprStr = ((SqlTableColumn.SqlComputedColumn) s).getExpr().toString();
                                    columnMap.put(columnName, exprStr);

                                } else {
                                    throw new FlinkSqlParseException("当前CREATE SQL解析有误,请联系开发人员");
                                }

                                return columnMap;

                            }).collect(Collectors.toList());

                            // 当前表创建来源信息,propertyList,这里不解析,要求此类表在元数据功能模块下创建
                            // 分区键 partitionKey
                            log.info(columnInfo.size() + "");

                        }
                    }
                }*/
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return sqlList;
    }

    @Test
    public void testFlinkSqlParse() {

        String sql = "CREATE TABLE db.T(\n"
                + "  a int,\n"
                + "  b varchar(20),\n"
                + "  c as my_udf(b),\n"
                + "  watermark for b as my_udf(b, 1) - INTERVAL '5' second\n"
                + ") WITH (\n"
                + "  'k1' = 'v1',\n"
                + "  'k2' = 'v2');\n"

                + " WITH t as (select a,b,c from T) select a from t ";

        String sql1 = "INSERT OVERWRITE other WITH t as (select a,b,c from T),v as (select d,e from V) select t.a,t.b,t.c,d.e from t join v on t.a = v.e ";

        String sql2 = "SELECT u.name,sum(o.amount) AS total\n" +
                "         FROM orders o\n" +
                "         INNER JOIN users u ON o.uid = u.id\n" +
                "         WHERE u.age < 27\n" +
                "         GROUP BY u.name\n";
        String sql3 = "CREATE  VIEW   MyTable (a,b)  AS   SELECT c,d FROM y";

        String sql4 = "INSERT OVERWRITE other SELECT a FROM t";

        //String sql5 = "CREATE  TABLE   MyTable   AS   SELECT a,b,c FROM y";

        String sql6 = "CREATE EXTERNAL TABLE `fs_table`(\n" +
                "  `user_id` string, \n" +
                "  `order_amount` double)\n" +
                "PARTITIONED BY ( \n" +
                "  `dt` string, \n" +
                "  `h` string, \n" +
                "  `m` string)\n" +
                "stored as ORC \n" +
                "TBLPROPERTIES (\n" +
                "  'sink.partition-commit.policy.kind'='metastore',\n" +
                "  'partition.time-extractor.timestamp-pattern'='$dt $h:$m:00'\n" +
                ")\n";

        String sql7 = " select t.a,t.b,t.c,v.e from (select a,b,c from T ) as t join v on t.a = v.e ";

        List<String> list = parseFlinkSql(sql7);
        log.info(list.size() + "");
    }


}
