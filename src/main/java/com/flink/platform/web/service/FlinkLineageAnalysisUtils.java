package com.flink.platform.web.service;

import com.flink.platform.web.exception.StreamNodeParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
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
        sourceNodes.forEach(FlinkLineageAnalysisUtils::analysisStreamNode);
        // 通过SinkNode分析数据sink
        sinkNodes.forEach(FlinkLineageAnalysisUtils::analysisStreamNode);

    }

    /**
     * 解析StreamNode,获取Source和Sink的相关配置信息
     *
     * @param streamNode 各算子节点StreamNode
     */
    private static void analysisStreamNode(StreamNode streamNode) {
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
                throw new StreamNodeParseException("解析StreamNode获取SourceFunction构造函数参数错误");
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
                    // 自定义SinkFunction,通过获取open方法的参数来解析信息
                    if (clazz.isAssignableFrom(RichSinkFunction.class)) {
                        RichSinkFunction function = (RichSinkFunction) ((StreamSink) operator).getUserFunction();
                        // 获取全局参数
                        // 这里需要固定化,不同的数据源对应不同的配置连接属性
                        ParameterTool tool = (ParameterTool) function.getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

                    }


                }

            } catch (Exception e) {
                throw new StreamNodeParseException("解析StreamNode获取SinkFunction构造函数参数错误");
            }


        } else {
            throw new StreamNodeParseException("当前节点算子既不是Source也不是Sink,暂不解析");
        }


    }


}
