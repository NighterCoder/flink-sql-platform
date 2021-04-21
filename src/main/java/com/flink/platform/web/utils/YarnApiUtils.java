package com.flink.platform.web.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.flink.platform.web.common.entity.HttpYarnApp;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Yarn 工具类
 * <p>
 * Created by 凌战 on 2021/4/21
 */
public class YarnApiUtils {

    private static final Logger LOG = LoggerFactory.getLogger(YarnApiUtils.class);

    private static final Map<String, String> HEADERS;

    static {
        HEADERS = new HashMap<>();
        // 实体头,表示发送端(客户端/服务器发送的实体数据类型)
        HEADERS.put("Content-Type", "application/json");
        // 请求头,表示发送端(客户端)希望接受的数据类型
        HEADERS.put("Accept", "application/json; charset=UTF-8");
    }

    private YarnApiUtils() {

    }

    /**
     * 获取活跃的yarn应用
     * <p>
     * 活跃的状态: new,new_saving,submitted,accepted,running
     *
     * @param yarnUrl yarn的url
     * @return List<HttpYarnApp>
     */
    public static List<HttpYarnApp> getActiveApps(String yarnUrl) {
        Map<String, Object> params = new HashMap<>();
        /**
         * todo 确认这里是sate
         */
        params.put("state", "new,new_saving,submitted,accepted,running");

        OkHttpUtils.Result result =
                OkHttpUtils.doGet(getAppsUrl(yarnUrl), params, HEADERS);
        if (result.isSuccessful && StringUtils.isNotEmpty(result.content)) {
            return parseAppsApiResponse(result);
        }
        return null;
    }


    /**
     * @param yarnUrl yarnUrl
     * @return 查找yarn app的url
     */
    private static String getAppsUrl(String yarnUrl) {
        return appendUrl(yarnUrl) + "ws/v1/cluster/apps";
    }


    /**
     * 给url加上/
     *
     * @param url url
     */
    private static String appendUrl(String url) {
        if (!url.endsWith("/")) {
            url += "/";
        }
        return url;
    }


    /**
     * 对返回Yarn App列表的json进行解析
     *
     * @param result
     * @return List<HttpYarnApp>
     */
    private static List<HttpYarnApp> parseAppsApiResponse(OkHttpUtils.Result result) {
        if (result.isSuccessful && StringUtils.isNotEmpty(result.content)) {
            JSONObject jsonObject = JSONObject.parseObject(result.content);
            if (jsonObject != null) {
                /**
                 * apps是一个JSONObject
                 */
                JSONObject apps = jsonObject.getJSONObject("apps");
                if (apps != null) {
                    String app = apps.getString("app");
                    if (StringUtils.isNotEmpty(app)) {
                        return JSON.parseArray(app, HttpYarnApp.class);
                    }
                }
            }
        }
        return new ArrayList<>();
    }


}
