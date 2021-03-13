package com.flink.platform.web.service;

import com.flink.platform.web.exception.StreamNodeParseException;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.springframework.stereotype.Service;

import java.lang.reflect.Constructor;
import java.util.Collection;

/**
 * Created by 凌战 on 2021/3/3
 */
@Service
public class LineageAnalysisService {


    /**
     * 支持对流式处理的Flink Jar包任务进行血缘分析
     * 目前只支持数据级别血缘解析,即source表 -> sink表
     *
     * @param streamGraph Flink底层StreamGraph图
     */
    public void flinkJarLineageAnalysis(StreamGraph streamGraph) {
        Collection<Integer> sourceIds = streamGraph.getSourceIDs();
        Collection<Integer> sinkIds = streamGraph.getSinkIDs();


    }

    /**
     * 解析StreamNode,获取Source和Sink的相关配置信息
     *
     * @param streamNode 各算子节点StreamNode
     */
    private void analysisStreamNode(StreamNode streamNode) throws IllegalAccessException, InstantiationException {
        StreamOperator operator = streamNode.getOperator();
        // 属于StreamSource
        if (operator instanceof StreamSource) {
            Constructor[] constructors = ((StreamSource) operator).getClass().getDeclaredConstructors();
            // StreamSource 只有一个构造函数,并且只要一个构造参数
            Class[] parameterTypes = constructors[0].getParameterTypes();
            try {
                if (parameterTypes[0].isAssignableFrom(SourceFunction.class)){

                }


            } catch (Exception e) {
                throw new StreamNodeParseException("解析StreamNode获取构造函数参数错误");
            }

        } else if (operator instanceof StreamSink) {
            // 属于StreamSink

        } else {
            throw new StreamNodeParseException("当前节点算子既不是Source也不是Sink,暂不解析");
        }


    }


}
