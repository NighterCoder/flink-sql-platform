package com.flink.platform.web.service;

import com.flink.platform.web.common.entity.JobSubmitDTO;
import com.flink.platform.web.common.entity.SessionVO;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.analysis.SessionDO;
import com.flink.platform.web.common.entity.login.LoginUser;
import com.flink.platform.web.common.enums.ExecuteType;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.enums.SessionType;
import com.flink.platform.web.common.util.SQLUtils;
import com.flink.platform.web.config.FlinkConfProperties;
import com.flink.platform.web.manager.SessionManager;
import com.flink.platform.web.manager.SessionManagerFactory;
import com.flink.platform.web.mapper.SessionMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by 凌战 on 2021/2/20
 */
@Slf4j
@Service
public class DataAnalysisService {


    @Autowired
    private SessionManagerFactory sessionManagerFactory;

    @Autowired
    private FlinkJobService flinkJobService;

    @Autowired
    private SessionMapper sessionMapper;


    /**
     * 获取Session
     *
     * @param st SessionType
     * @param et ExecuteType
     */
    public SessionVO getSession(SessionType st, ExecuteType et) {

//        LoginUser loginUser = (LoginUser) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
//        String username = loginUser.getUsername();
        String username = "admin";

        // 工厂模式创建SessionManager
        // 1.根据SessionType判断是FlinkSessionManager或者SparkSessionManager
        SessionManager sessionManager = sessionManagerFactory.create(st);

        // 2.根据用户名,sessionType,executeType来查询sessionId
        SessionDO sessionDO = sessionMapper.selectByConditions(username, st.getCode(), et.getCode());

        String sessionId;
        // 3.sessionId不存在,或者当前对应sessionId的session失效,重新创建
        if (sessionDO == null || sessionManager.statusSession(sessionDO.getSessionId()).equals(SessionState.NONE)) {
            sessionId = sessionManager.createSession(username + "的session",
                    et.getType()
                    
            );
            if (sessionDO != null) {
                // 先将旧记录逻辑删除
                sessionMapper.deleteSessionByConditions(username, st.getCode(), et.getCode());
            }
            // 添加新记录
            sessionMapper.insert(
                    new SessionDO()
                            .setUsername(username)
                            .setSessionType(st.getCode())
                            .setExecuteType(et.getCode())
                            .setSessionId(sessionId)
                            .setDeleted(0)
            );
        } else {
            sessionId = sessionDO.getSessionId();
        }
        // todo 获取applicationMasterUrl
        return new SessionVO(sessionId, SessionState.RUNNING, "");
    }


    /**
     * 数据分析页面批量提交SQL
     * 该场景下支持
     * 1.Flink: yarn-session模式提交任务,不建议使用yarn-per-job,故前端没有放开
     * 2.Spark: 使用Apache Livy的livy-session模式
     * @param dto dto
     */
    public StatementResult submit(JobSubmitDTO dto) {
        // todo 保存当前user执行的SQL,以便查询历史记录

        String sql = dto.getSql();
        String sessionId = dto.getSessionId();
        // 工厂模式创建SessionManager
        SessionManager sessionManager = sessionManagerFactory.create(dto.getSessionType());
        // 格式化SQL 1.去除注释 2.去除空格
        sql = SQLUtils.commentsFormat(sql);
        // todo 目前没有支持变量替换
        sql = SQLUtils.generateSQL(sql, Collections.emptyMap());
        log.info("提交SQL:{}", sql);

        // 对SQL按照分号作为分隔符分割
        List<String> list = Arrays.stream(sql.split(";"))
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toList());

        int size = list.size();
        for (int i = 0; i < size; i++) {
            // 只返回最后一个语句的执行结果
            if (i == size - 1) {
                return sessionManager.submit(list.get(i), sessionId);
            }
            sessionManager.submit(list.get(i), sessionId);
        }
        return new StatementResult();
    }

    /**
     * 获取查询结果
     *
     * @param dto dto
     */
    public StatementResult fetchData(JobSubmitDTO dto) {
        String sessionId = dto.getSessionId();
        String jobID = dto.getJobId();
        long token = dto.getToken();


        return null;

    }


}
