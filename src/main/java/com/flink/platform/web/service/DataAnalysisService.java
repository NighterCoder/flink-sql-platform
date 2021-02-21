package com.flink.platform.web.service;

import com.flink.platform.web.common.entity.JobSubmitDTO;
import com.flink.platform.web.common.entity.SessionVO;
import com.flink.platform.web.common.entity.StatementResult;
import com.flink.platform.web.common.entity.analysis.SessionDO;
import com.flink.platform.web.common.enums.ExecuteType;
import com.flink.platform.web.common.enums.SessionState;
import com.flink.platform.web.common.enums.SessionType;
import com.flink.platform.web.manager.SessionManager;
import com.flink.platform.web.manager.SessionManagerFactory;
import com.flink.platform.web.mapper.SessionMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;

/**
 * Created by 凌战 on 2021/2/20
 */
@Service
public class DataAnalysisService {


    @Autowired
    private SessionManagerFactory sessionManagerFactory;


    @Autowired
    private SessionMapper sessionMapper;


    /**
     * 获取Session
     *
     * @param st SessionType
     * @param et ExecuteType
     */
    public SessionVO getSession(SessionType st, ExecuteType et) {
        // 1.todo 首先获取当前登录用户的用户名
        String username = "test1";
        // 工厂模式创建SessionManager
        SessionManager sessionManager = sessionManagerFactory.create(st);
        // 2.根据用户名,sessionType,executeType来查询sessionId
        SessionDO sessionDO = sessionMapper.selectByConditions(username, st.getCode(), et.getCode());
        String sessionId;
        // 3.sessionId不存在,或者当前对应sessionId的session失效,重新创建
        if (sessionDO == null || sessionManager.statusSession(sessionDO.getSessionId()).equals(SessionState.NONE)) {
            sessionId = sessionManager.createSession(username + "的session", null, et.getType(), Collections.emptyMap());
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
     * 批量提交SQL
     *
     * @param dto dto
     */
    public StatementResult submit(JobSubmitDTO dto) {
        // todo 保存当前user执行的SQL,以便查询历史记录

        String sql=dto.getSql();
        SessionManager sessionManager=sessionManagerFactory.create(dto.getSessionType());
        // 格式化SQL 1.去除注释 2.去除空格




    }


}
