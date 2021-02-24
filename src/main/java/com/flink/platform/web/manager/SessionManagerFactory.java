package com.flink.platform.web.manager;

import com.flink.platform.web.common.enums.SessionType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 工厂模式,根据不同的SessionType构建对应不同SessionManager
 * <p>
 * Created by 凌战 on 2021/2/20
 */
@Component
public class SessionManagerFactory {

    @Autowired
    private FlinkSessionManager flinkSessionManager;

    @Autowired
    private SparkSessionManager sparkSessionManager;

    public SessionManager create(SessionType st) {
        switch (st) {
            case FLINK:
                return flinkSessionManager;
            case SPARK:
                return sparkSessionManager;
            default:
                throw new RuntimeException("unknown instance type");
        }
    }


}
