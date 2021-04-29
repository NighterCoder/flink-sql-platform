package com.flink.platform.web.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.flink.platform.web.common.entity.BackpressureInfo;
import com.flink.platform.web.common.entity.HttpYarnApp;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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


    /**
     * flink 判断是否存在运行中的job
     *
     * @param yarnUrl
     * @param appId
     * @return
     */
    public static boolean existRunningJobs(String yarnUrl, String appId) {
        // 获取jobId
        String url = appendUrl(yarnUrl) + "proxy/%s/jobs";
        url = String.format(url, appId);
        OkHttpUtils.Result result = OkHttpUtils.doGet(url, null, HEADERS);
        if (result.isSuccessful && StringUtils.isNotEmpty(result.content)) {
            try {
                JSONArray jobs = JSON.parseObject(result.content).getJSONArray("jobs");
                if (jobs != null) {
                    for (int i = 0; i < jobs.size(); i++) {
                        JSONObject job = jobs.getJSONObject(i);
                        if ("RUNNING".equals(job.get("status"))) {
                            return true;
                        }
                    }
                }
                //for 1.4 version
                jobs = JSON.parseObject(result.content).getJSONArray("jobs-running");
                return jobs != null && jobs.size() > 0;
            } catch (JSONException e) {
                //未处于运行状态的APP会返回html信息的问题
            }
        }
        //请求失败判定为存在
        return true;
    }

    /**
     * 杀掉Yarn上的任务
     *
     * @param yarnUrl
     * @param appId
     * @return
     */
    public static boolean killApp(String yarnUrl, String appId) {
        String stateUrl = getAppsUrl(yarnUrl) + "/" + appId + "/state";
        OkHttpUtils.Result result = OkHttpUtils.doPut(stateUrl, OkHttpUtils.MEDIA_JSON, "{\"state\": \"KILLED\"}", HEADERS);
        if (result.isSuccessful && StringUtils.isNotEmpty(result.content)) {
            JSONObject jsonObject = JSON.parseObject(result.content);
            String state = jsonObject.getString("state");
            return StringUtils.isNotEmpty(state);
        }
        return false;
    }


    /**
     * flink 背压监测阻塞任务数
     * 上游算子的生产速度快于下游算子的消费速度
     * <p>
     * OK：0 <= Ratio <= 0.10
     * LOW：0.10 <Ratio <= 0.5
     * HIGH：0.5 <Ratio <= 1
     *
     * @param yarnUrl
     * @param appId
     */
    public static BackpressureInfo backpressure(String yarnUrl, String appId) {
        // 获取运行着的jobId
        String url = appendUrl(yarnUrl) + "proxy/%s/jobs";
        url = String.format(url, appId);
        OkHttpUtils.Result result = OkHttpUtils.doGet(url, null, HEADERS);
        if (result.isSuccessful && StringUtils.isNotEmpty(result.content)) {
            try {
                // job列表,per job模式正常只有一个
                String id = null;
                JSONArray jobs = JSON.parseObject(result.content).getJSONArray("jobs");
                if (jobs != null) {
                    JSONObject job = jobs.getJSONObject(0);
                    if (job != null && "RUNNING".equals(job.get("status"))) {
                        id = job.getString("id");
                    }
                }
                /**
                 * 特殊处理 Flink 1.4版本
                 */
                if (id == null) {
                    jobs = JSON.parseObject(result.content).getJSONArray("jobs-running");
                    if (jobs != null && jobs.size() > 0) {
                        id = jobs.getString(0);
                    }
                }
                /**
                 * 拿到具体Flink任务的job id
                 */
                if (id != null) {
                    url += "/" + id;
                    result = OkHttpUtils.doGet(url, null, HEADERS);
                    /**
                     * jid,
                     * name,
                     * isStoppable,
                     * state,
                     * start-time,
                     * end-time,
                     * duration,
                     * now,
                     * timestamp:{}
                     * vertices:{
                     *     id:
                     *     name:
                     *     parallelism:
                     *     status:
                     *     start-time:
                     *     end-time:
                     *     duration
                     *     tasks:{}
                     *     metrics:{}
                     * }
                     * status-counts:{}
                     * plan:{}
                     */
                    if (result.isSuccessful && StringUtils.isNotEmpty(result.content)) {
                        JSONObject jobDetails = JSON.parseObject(result.content);
                        JSONArray vertices = jobDetails.getJSONArray("vertices");
                        for (int i = vertices.size() - 1; i >= 0; i--) {
                            // 获取顶点即最后一个算子的背压监控数据
                            JSONObject vertexObject = vertices.getJSONObject(i);
                            /**
                             * 从最后一个算子开始算
                             */
                            String vertexId = vertexObject.getString("id");
                            String backPressureUrl = url + "/vertices/" + vertexId + "/backpressure";
                            JSONArray subtasks = null;
                            int count = 0;
                            for (; ; ) {
                                count++;
                                // 第一次请求 status:deprecated,无数据
                                result = OkHttpUtils.doGet(backPressureUrl, null, HEADERS);
                                if (result.isSuccessful && StringUtils.isNotEmpty(result.content)
                                        && JSON.parseObject(result.content).getJSONArray("subtasks") != null) {
                                    /**
                                     * {
                                     *  "status":"ok",
                                     *  "backpressure-level":"ok",
                                     *  "end-timestamp":1619418503645,
                                     *  "subtasks":[
                                     *      {"subtask":0,"backpressure-level":"ok","ratio":0.0},
                                     *      {"subtask":1,"backpressure-level":"ok","ratio":0.0},
                                     *      {"subtask":2,"backpressure-level":"ok","ratio":0.0},
                                     *      {"subtask":3,"backpressure-level":"ok","ratio":0.0},
                                     *      {"subtask":4,"backpressure-level":"ok","ratio":0.0}
                                     *      ]
                                     *  }
                                     */
                                    subtasks = JSON.parseObject(result.content).getJSONArray("subtasks");
                                    if (subtasks != null) {
                                        break;
                                    }
                                    Thread.sleep(2000);
                                }
                                // 重试十次无结果就返回
                                if (count > 10) {
                                    break;
                                }
                            }

                            if (subtasks != null) {
                                for (int j = 0; j < subtasks.size(); j++) {
                                    /**
                                     * plan:{
                                     *    jid:
                                     *    name:
                                     *    nodes:[{
                                     *        id,
                                     *        parallelism,
                                     *        operator,
                                     *        operator_strategy,
                                     *        description: 算子名称
                                     *        input:[
                                     *          {
                                     *              num:0,
                                     *              id:
                                     *              ship_strategy:
                                     *              exchange
                                     *          },...
                                     *        ],
                                     *        optimizer_properties
                                     *    },...
                                     *   ]
                                     * }
                                     *
                                     *
                                     */
                                    JSONObject subtask = subtasks.getJSONObject(i);
                                    // 大于0
                                    if (subtask.getDouble("ratio") > 0) {
                                        Set<String> names = new HashSet<>();
                                        JSONArray nodes = jobDetails.getJSONObject("plan").getJSONArray("nodes");
                                        for (int k = 0; k < nodes.size(); k++) {
                                            JSONObject node = nodes.getJSONObject(0);
                                            // inputs
                                            JSONArray jsonArray = node.getJSONArray("inputs");
                                            // 寻找输入节点为vertexId的节点id
                                            if (jsonArray != null) {
                                                jsonArray.forEach(item -> {
                                                    if (vertexId.equals(((JSONObject) item).getString("id"))) {
                                                        /**
                                                         * 获取到算子的名称
                                                         */
                                                        names.add(node.getString("description").replaceAll("-&gt;", "->"));
                                                    }
                                                });
                                            }
                                        }

                                        /**
                                         * 如果是最后一个算子发生背压,那么最后就是vertexObject.getString("name");
                                         * 如果不是最后一个算子,找出节点输入源为当前算子的节点名称: 例如 A->B A->C, 如果A的某个子线程ratio>0,那么B和C都会告警
                                         */
                                        if (!names.isEmpty()) {
                                            return new BackpressureInfo((int) (subtask.getDouble("ratio") * 100), org.apache.commons.lang.StringUtils.join(names, ","));
                                        } else {
                                            return new BackpressureInfo((int) (subtask.getDouble("ratio") * 100), vertexObject.getString("name"));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (JSONException e) {
                // 未处于运行状态中的APP会返回html信息的问题
            } catch (Exception e) {
                LOG.error("backpressure execute error: " + e.getMessage(), e);
            }
        }
        return null;
    }


    /**
     * 获取最后一次提交的未处于运行状态的应用
     *
     * @param yarnUrl
     * @param user
     * @param queue
     * @param name
     * @param retries
     */
    public static HttpYarnApp getLastNoActiveApp(String yarnUrl, String user, String queue, String name, int retries) {
        if (!queue.startsWith("root.")) {
            queue = "root." + queue;
        }
        Map<String, Object> params = new HashMap<>();
        params.put("user", user);
        params.put("state", "finished,killed,failed");
        for (; ; ) {
            OkHttpUtils.Result result = OkHttpUtils.doGet(getAppsUrl(yarnUrl), params, HEADERS);
            List<HttpYarnApp> appList = parseAppsApiResponse(result);
            if (!appList.isEmpty()) {
                appList.sort((app1, app2) -> {
                    long time1 = app1.getFinishedTime();
                    long time2 = app2.getFinishedTime();
                    return Long.compare(time2, time1);
                });
                for (HttpYarnApp httpYarnApp : appList) {
                    if (httpYarnApp.getQueue().equals(queue) && httpYarnApp.getName().equals(name)) {
                        return httpYarnApp;
                    }
                }
            }
            if (retries <= 0) {
                break;
            }
            retries--;
        }
        return null;
    }


    /**
     * 获取运行中的应用
     * @param yarnUrl
     * @param user
     * @param queue
     * @param name
     * @param retries
     * @return
     */
    public static HttpYarnApp getActiveApp(String yarnUrl,String user,String queue,String name,int retries){
        Map<String,Object> params = new HashMap<>();
        params.put("user",user);
        params.put("queue",queue);
        params.put("state","new,new_saving,submitted,accepted,running");
        for (;;){
            OkHttpUtils.Result result = OkHttpUtils.doGet(getAppsUrl(yarnUrl),params,HEADERS);
            List<HttpYarnApp> apps =  parseAppsApiResponse(result);
            if (!apps.isEmpty()){
                //按照应用执行开始时间排序
                apps.sort((app1,app2) -> {
                    long time1 = app1.getStartedTime();
                    long time2 = app2.getStartedTime();
                    return Long.compare(time2,time1);
                });
                for (HttpYarnApp httpYarnApp:apps){
                    if (httpYarnApp.getName().equals(name)){
                        return httpYarnApp;
                    }
                }
            }

            if (retries <= 0){
                break;
            }
            retries -- ;
        }
        return null;
    }





}
