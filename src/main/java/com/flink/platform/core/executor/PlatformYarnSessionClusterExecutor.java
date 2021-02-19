package com.flink.platform.core.executor;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;


/**
 * PlatformAbstractSessionClusterExecutor 的 on yarn实现
 * Created by 凌战 on 2021/2/19
 */
public class PlatformYarnSessionClusterExecutor extends PlatformAbstractSessionClusterExecutor<ApplicationId, YarnClusterClientFactory> {

    public static final String NAME = "yarn-session";

    public PlatformYarnSessionClusterExecutor(ExecutionContext executionContext) {
        super(new YarnClusterClientFactory());
        this.executionContext = executionContext;
    }
}
