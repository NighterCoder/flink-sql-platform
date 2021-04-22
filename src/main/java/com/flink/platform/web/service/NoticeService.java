package com.flink.platform.web.service;

/**
 * 告警接口:
 *
 * 1. 邮件告警
 * 2. 钉钉告警
 * 3. 电话告警
 *
 * Created by 凌战 on 2021/4/22
 */
public interface NoticeService {

    void sendEmail(String to,String content);

    /**
     * 发送公共群告警信息
     * @param ats 被@的对象的相关信息
     *            1.atMobiles 手机
     *            2.atUserIds 用户id
     *            3.isAtAll 是否@全部
     *            这里用手机号为主
     * @param content
     */
    void sendDingding(String[] ats,String content);


    /**
     * 发送指定群
     * @param token 对应指定群的token
     * @param ats
     * @param content
     */
    void sendDingding(String token,String[] ats,String content);


    /**
     * 是否公共群
     * 在配置文件中配置token,并且以逗号相隔
     * @param token
     * @return
     */
    boolean isWatcherToken(String token);

}
