package com.flink.platform.web.service.impl;

import com.alibaba.fastjson.JSON;
import com.flink.platform.web.common.SystemConstants;
import com.flink.platform.web.config.DingdingConfig;
import com.flink.platform.web.service.NoticeService;
import com.flink.platform.web.utils.OkHttpUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by 凌战 on 2021/4/22
 */
@Service
public class NoticeServiceImpl implements NoticeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NoticeServiceImpl.class);
    private static final Pattern MAIL_PATTERN = Pattern.compile("^\\w+((-\\w+)|(\\.\\w+))*\\@[A-Za-z0-9]+((\\.|-)[A-Za-z0-9]+)*\\.[A-Za-z0-9]+$");

    @Value("${spring.mail.username}")
    private String from;


    @Autowired
    private DingdingConfig dingdingConfig;


    @Override
    public void sendEmail(String to, String content) {

    }

    @Override
    public void sendDingding(String[] ats, String content) {
        if (!dingdingConfig.isEnabled()) {
            LOGGER.error("dingding alarm is not enabled, use console\n" + content);
            return;
        }
        if (StringUtils.isBlank(dingdingConfig.getWatcherToken())) {
            LOGGER.error("dingding public watch token is not set, use console\n" + content);
        }
        sendDingding(dingdingConfig.getWatcherToken(), ats, content);
    }

    @Override
    public void sendDingding(String token, String[] ats, String content) {
        if (!dingdingConfig.isEnabled()) {
            LOGGER.error("dingding alarm is not enabled, use console\n" + content);
            return;
        }
        Map<String, Object> reqBody = new HashMap<>();
        /**
         * 以普通Text类型发送
         */
        reqBody.put("msgtype", "text");
        reqBody.put("content", content);
        if (ats != null && ats.length > 0) {
            Map<String, Object> atMap = new HashMap<>();
            atMap.put("isAtAll", false);
            atMap.put("atMobiles", ats);
            reqBody.put("at", atMap);
        }

        OkHttpUtils.doPost(SystemConstants.DINGDING_ROBOT_URL + token,
                OkHttpUtils.MEDIA_JSON,
                JSON.toJSONString(reqBody),
                null);
    }

    @Override
    public boolean isWatcherToken(String token) {
        return dingdingConfig.isEnabled() &&
                dingdingConfig.getWatcherToken() != null &&
                dingdingConfig.getWatcherToken().equals(token.split("&")[0]);
    }
}
