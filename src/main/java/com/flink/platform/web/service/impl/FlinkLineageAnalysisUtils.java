package com.flink.platform.web.service.impl;

import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.common.entity.lineage.LineageVO;
import com.flink.platform.web.exception.FlinkSqlParseException;
import com.flink.platform.web.exception.StreamNodeParseException;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;
import org.apache.flink.sql.parser.ddl.SqlCreateView;
import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImpl;
import org.apache.flink.sql.parser.validate.FlinkSqlConformance;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.avatica.util.Quoting.BACK_TICK;

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
     *
     * Flink SQL目前可以通过 SET table.sql-dialect=default/hive 来动态切换语义;
     *
     *
     * @param statement SQL语句
     */
    public static void sqlLineageAnalysis(String statement) {

        if (statement != null && !statement.isEmpty()) {
            try {


                SqlParser parser = SqlParser.create(statement, SqlParser.configBuilder()
                        // 使用FlinkSqlParse工厂
                        .setParserFactory(FlinkSqlParserImpl.FACTORY)
                        .setQuoting(BACK_TICK)
                        .setUnquotedCasing(Casing.TO_LOWER)   //字段名统一转化为小写
                        .setQuotedCasing(Casing.UNCHANGED)
                        .setConformance(FlinkSqlConformance.DEFAULT)
                        .build()
                );



                List<SqlNode> sqlNodeList = parser.parseStmtList().getList();

                List<LineageVO> input = new ArrayList<>();
                List<LineageVO> output = new ArrayList<>();



                // CREATE,INSERT
                if (sqlNodeList != null && !sqlNodeList.isEmpty()) {
                    for (SqlNode sqlNode : sqlNodeList) {
                        // todo 补充其他SqlNode信息

                        // todo 1.CreateTable 类型一般是 SqlCreateHiveTable 或者 带有with信息
                        if (sqlNode instanceof SqlCreateTable) {
                            // 这里解析的是output的表名
                            String tableName = ((SqlCreateTable) sqlNode).getTableName().toString();
                            // 当前表的字段列表
                            // SqlNode -> SqlTableColumn -> (SqlRegularColumn,SqlComputedColumn)
                            List<Map<String,String>> columnInfo =  ((SqlCreateTable) sqlNode).getColumnList().getList().stream().map(s -> {

                                Map<String,String> columnMap = new HashMap<>();

                                if (s instanceof SqlTableColumn.SqlRegularColumn) {
                                    String columnName = ((SqlTableColumn.SqlRegularColumn) s).getName().toString();
                                    String columnType = ((SqlTableColumn.SqlRegularColumn) s).getType().getTypeNameSpec().getTypeName().toString();
                                    columnMap.put(columnName,columnType);

                                } else if (s instanceof SqlTableColumn.SqlComputedColumn) {
                                    String columnName = ((SqlTableColumn.SqlComputedColumn) s).getName().toString();
                                    String exprStr = ((SqlTableColumn.SqlComputedColumn) s).getExpr().toString();
                                    columnMap.put(columnName,exprStr);

                                } else {
                                    throw new FlinkSqlParseException("当前CREATE SQL解析有误,请联系开发人员");
                                }

                                return columnMap;
                            }).collect(Collectors.toList());


                            // todo 根据是普通的CreateTable或者SqlCreateHiveTable
                            if (sqlNode instanceof SqlCreateHiveTable){
                                //((SqlCreateHiveTable)sqlNode)


                            }else{


                            }






                            // 当前表创建来源信息,propertyList,这里不解析,要求此类表在元数据功能模块下创建
                            // 分区键 partitionKey
                            LineageVO lineageVO = new LineageVO(tableName,columnInfo);
                            input.add(lineageVO);

                        } else if (sqlNode instanceof SqlCreateView) {

                            String viewName = ((SqlCreateView) sqlNode).getViewName().toString();
                           /* List<Map<String,String>> columnInfo = ((SqlCreateView) sqlNode).getFieldList().getList().stream().map(s -> {

                                Map<String,String> columnMap = new HashMap<>();

                                // if ()



                            });*/


                        }


                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }


}
