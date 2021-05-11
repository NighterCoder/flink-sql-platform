package com.flink.platform.core.executor.v2.factory;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.client.deployment.ClusterDescriptor;
import org.apache.flink.configuration.Configuration;

/**
 * Created by 凌战 on 2021/5/11
 */
public enum YarnClusterClientFactory implements AbstractClusterClientFactory {

    INSTANCE;


    @Override
    public ClusterDescriptor createClusterDescriptor(String yarnConfDir, Configuration flinkConfig) {

        if (StringUtils.isNotBlank(yarnConfDir)){
            try{




            }catch (Exception e){
                throw new RuntimeException(e);
            }


        }





        return null;
    }






}
