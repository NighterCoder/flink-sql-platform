package com.flink.platform.web.service.impl;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.entity2table.NodeExecuteHistory;
import com.flink.platform.web.common.entity.lineage.ColumnVO;
import com.flink.platform.web.common.entity.lineage.TableLineageInputOutput;
import com.flink.platform.web.exception.FlinkSqlParseException;
import com.flink.platform.web.exception.StreamNodeParseException;
import com.flink.platform.web.manager.SqlParserUtils;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.SqlTableOption;
import org.apache.flink.sql.parser.dml.RichSqlInsert;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.table.api.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 目前支持对
 * 1. Flink流处理Jar包的血缘分析 [表级别-> source表到sink表]
 * todo 2. Flink SQL流处理或者批处理的血缘分析 [字段级别]
 * <p>
 * Created by 凌战 on 2021/3/3
 */
public class FlinkLineageAnalysisUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkLineageAnalysisUtils.class);

    /**
     * 支持对流式处理的Flink Jar包任务进行血缘分析
     * 目前只支持数据级别血缘解析,即source表 -> sink表
     *
     * @param streamGraph Flink底层StreamGraph图
     */
    public static void streamJarLineageAnalysis(StreamGraph streamGraph) {
        Collection<Integer> sourceIds = streamGraph.getSourceIDs();
        Collection<Integer> sinkIds = streamGraph.getSinkIDs();
        // 遍历source和sink
        Collection<StreamNode> sourceNodes = sourceIds.stream()
                .map(streamGraph::getStreamNode)
                .collect(Collectors.toList());
        Collection<StreamNode> sinkNodes = sinkIds.stream()
                .map(streamGraph::getStreamNode)
                .collect(Collectors.toList());
        // 通过SourceNode分析数据源
        sourceNodes.forEach(streamNode -> {
            analysisStreamNode(streamGraph, streamNode);
        });
        // 通过SinkNode分析数据sink
        sinkNodes.forEach(streamNode -> {
            analysisStreamNode(streamGraph, streamNode);
        });

    }

    /**
     * 解析StreamNode,获取Source和Sink的相关配置信息
     *
     * @param streamNode 各算子节点StreamNode
     */
    private static void analysisStreamNode(StreamGraph streamGraph, StreamNode streamNode) {
        StreamOperator operator = streamNode.getOperator();
        // 属于StreamSource
        if (operator instanceof StreamSource) {
            Constructor[] constructors = ((StreamSource) operator).getClass().getDeclaredConstructors();
            // StreamSource 只有一个构造函数,并且只有一个构造参数即SourceFunction
            Class[] parameterTypes = constructors[0].getParameterTypes();
            Class clazz = parameterTypes[0];
            try {
                if (clazz.isAssignableFrom(SourceFunction.class)) {
                    LOG.info("解析Stream Jar的Source");
                    // SourceFunction有很多实现类
                    if (clazz.isAssignableFrom(FlinkKafkaConsumerBase.class)) {
                        FlinkKafkaConsumerBase function = (FlinkKafkaConsumerBase) (((StreamSource) operator).getUserFunction());
                        Class funcClass = function.getClass();
                        // 默认getDeclaredFields是获取不到父类的属性的,需要循环遍历
                        OUT:
                        while (funcClass != null) {
                            Field[] declaredFields = funcClass.getDeclaredFields();
                            for (Field field : declaredFields) {
                                if (field.getName().equals("properties")) {
                                    field.setAccessible(true);
                                    Properties props = (Properties) field.get(function);
                                    String brokers = props.getProperty("bootstrap.servers");
                                    LOG.info("broker信息为" + brokers);
                                }
                                //最上层类存在
                                if (field.getName().equals("topicsDescriptor")) {
                                    field.setAccessible(true);
                                    KafkaTopicsDescriptor descriptor = (KafkaTopicsDescriptor) field.get(function);
                                    // todo
                                    List<String> topics = descriptor.getFixedTopics();
                                    LOG.info("topic信息为" + StringUtils.join(topics, ","));
                                    // 跳出循环
                                    break OUT;
                                }
                            }
                            funcClass = funcClass.getSuperclass();
                        }
                    }
                    // todo 添加其他数据源信息获取
                }


            } catch (Exception e) {
                throw new StreamNodeParseException("解析StreamNode获取SourceFunction构造函数参数错误", e);
            }

        } else if (operator instanceof StreamSink) {
            // 属于StreamSink
            Constructor[] constructors = ((StreamSink) operator).getClass().getDeclaredConstructors();
            // StreamSource 只有一个构造函数,并且只有一个构造参数即SourceFunction
            Class[] parameterTypes = constructors[0].getParameterTypes();
            Class clazz = parameterTypes[0];
            try {
                if (clazz.isAssignableFrom(SinkFunction.class)) {
                    LOG.info("解析Stream Jar的Sink");
                    // SinkFunction有很多实现类
                    // 1.自定义SinkFunction,首先固定在编写jar包时,将sink的信息用固定属性封装,使用GlobalJobParameters加载
                    if (clazz.isAssignableFrom(RichSinkFunction.class)) {
                        RichSinkFunction function = (RichSinkFunction) ((StreamSink) operator).getUserFunction();
                        // 获取全局参数
                        // 这里需要固定化,不同的数据源对应不同的配置连接属性
                        ParameterTool parameterTool = (ParameterTool) streamGraph.getExecutionConfig().getGlobalJobParameters();
                        String driver = parameterTool.get(SystemConstants.SINK_DRIVER);
                        String url = parameterTool.get(SystemConstants.SINK_URL);
                        String table = parameterTool.get(SystemConstants.SINK_TABLE);

                        LOG.info(driver);
                    } else if (clazz.isAssignableFrom(FlinkKafkaProducerBase.class)) {
                        // 2.使用Kafka作为Producer

                    }
                }
            } catch (Exception e) {
                throw new StreamNodeParseException("解析StreamNode获取SinkFunction构造函数参数错误", e);
            }
        } else {
            throw new StreamNodeParseException("当前节点算子既不是Source也不是Sink,暂不解析");
        }
    }


    /**
     * 对Flink SQL语句进行解析
     * todo 1.Query(select,insert into,insert overwrite)  对应  SqlInsert RichSqlInsert
     * todo 2.CreateTable 对应 SqlCreateTable
     * todo 3.CreateTableAsSelect 对应 // Flink SQL没有create table as
     * todo 4.DropTable 对应 SqlDropTable
     * todo 5.CreateView 对应 SqlCreateView // CreateViewAsSelect
     * todo 6.AlterView 对应 SqlAlterView
     *
     * @param stmt
     * @param sqlDialect
     * @param nodeExecuteHistory
     * @throws SqlParseException
     */
    public static void sqlLineageAnalysis(String stmt, SqlDialect sqlDialect, NodeExecuteHistory nodeExecuteHistory) throws SqlParseException {

        SqlParser parser;

        switch (sqlDialect) {
            case HIVE:
                parser = SqlParserUtils.getFlinkHiveSqlParser(stmt);
                break;
            case DEFAULT:
                parser = SqlParserUtils.getFlinkSqlParser(stmt);
                break;
            default:
                throw new FlinkSqlParseException("当前Flink语义不明,无法解析血缘关系!");
        }
        // 这里是单条SQL语句
        SqlNode sqlNode = parser.parseStmt();
        if (sqlNode instanceof SqlCreateTable) {
            // !!! 在建表时需要加上库名
            String tableName = ((SqlCreateTable) sqlNode).getTableName().toString();
            // 构建 Table 实体类
            TableLineageInputOutput tableLineageInputOutput = new TableLineageInputOutput();
            tableLineageInputOutput.setDb(tableName.split(".").length > 1 ? tableName.split(".")[0] : null);
            tableLineageInputOutput.setTable(tableName.split(".").length > 1 ? tableName.split(".")[1] : tableName);
            tableLineageInputOutput.setOwnerId(nodeExecuteHistory.getCreateBy());

            if (sqlNode instanceof SqlCreateHiveTable) {
                tableLineageInputOutput.setDbType("hive");
            } else {
                tableLineageInputOutput.setDbType("normal");
            }

            List<SqlNode> propInfo = ((SqlCreateTable) sqlNode).getPropertyList().getList();
            Map<String, String> propsMap = new HashMap<>();
            if (CollectionUtils.isNotEmpty(propInfo)) {
                for (SqlNode node : propInfo) {
                    propsMap.put(((SqlTableOption) node).getKeyString(),
                            ((SqlTableOption) node).getValueString()
                    );
                }
            }
            tableLineageInputOutput.setProps(propsMap);

            // 当前表的字段列表
            // SqlNode -> SqlTableColumn -> (SqlRegularColumn,SqlComputedColumn)
            List<ColumnVO> columnInfo = ((SqlCreateTable) sqlNode).getColumnList().getList().stream().map(s -> {
                ColumnVO columnVO = new ColumnVO();
                columnVO.setTable(tableLineageInputOutput);
                if (s instanceof SqlTableColumn.SqlRegularColumn) {
                    String columnName = ((SqlTableColumn.SqlRegularColumn) s).getName().toString();
                    String columnType = ((SqlTableColumn.SqlRegularColumn) s).getType().getTypeNameSpec().getTypeName().toString();
                    columnVO.setCol(columnName);
                    columnVO.setFullCol(tableName + "." + columnName);
                    columnVO.setType(columnType);
                } else if (s instanceof SqlTableColumn.SqlComputedColumn) {
                    String columnName = ((SqlTableColumn.SqlComputedColumn) s).getName().toString();
                    String exprStr = ((SqlTableColumn.SqlComputedColumn) s).getExpr().toString();
                    columnVO.setCol(columnName);
                    columnVO.setFullCol(tableName + "." + columnName);
                    columnVO.setExprStr(exprStr);
                } else {
                    throw new FlinkSqlParseException("当前CREATE SQL解析有误,请联系开发人员");
                }
                return columnVO;
            }).collect(Collectors.toList());

            // todo 封装
        } else if (sqlNode instanceof SqlCreateView) {

            // view 信息
            String viewName = ((SqlCreateView) sqlNode).getViewName().toString();
            List<String> columns = ((SqlCreateView) sqlNode).getFieldList().getList()
                    .stream()
                    .map(SqlNode::toString)
                    .collect(Collectors.toList());

            // as后面的信息
            SqlSelect sqlSelect = (SqlSelect) ((SqlCreateView) sqlNode).getQuery();
            String fromTable = sqlSelect.getFrom().toString();
            List<String> fromTableColumns = sqlSelect.getSelectList().getList()
                    .stream()
                    .map(SqlNode::toString)
                    .collect(Collectors.toList());

            //todo 封装
        } else if (sqlNode instanceof SqlInsert) {
            // insert 后面可以有很多写法
            String toTable = ((SqlInsert) (sqlNode)).getTargetTable().toString();
            // todo keywords
            SqlNode source = ((SqlInsert) (sqlNode)).getSource();
            // 1. insert + with select
            if (source instanceof SqlWith) {
                SqlNodeList withList = ((SqlWith) source).withList;
                withList.getList().stream().map(s -> {
                    String withName = ((SqlWithItem) s).name.toString();
                    SqlSelect sqlSelect = (SqlSelect) ((SqlWithItem) s).query;
                    String fromTable = sqlSelect.getFrom().toString();
                    List<String> fromTableColumns = sqlSelect.getSelectList().getList()
                            .stream()
                            .map(SqlNode::toString)
                            .collect(Collectors.toList());


                });

            }


        }


    }


}
