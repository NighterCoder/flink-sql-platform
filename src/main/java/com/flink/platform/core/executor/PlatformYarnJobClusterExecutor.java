package com.flink.platform.core.executor;

import com.flink.platform.core.context.ExecutionContext;
import org.apache.flink.yarn.YarnClusterClientFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * Created by 凌战 on 2021/2/19
 */
public class PlatformYarnJobClusterExecutor extends PlatformAbstractJobClusterExecutor<ApplicationId, YarnClusterClientFactory>  {

    public static final String NAME = "yarn-per-job";

    public PlatformYarnJobClusterExecutor(ExecutionContext executionContext) {
        super(new YarnClusterClientFactory());
        this.executionContext=executionContext;
    }
}
