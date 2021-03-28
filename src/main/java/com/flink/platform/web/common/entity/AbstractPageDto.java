package com.flink.platform.web.common.entity;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * 分页实体类
 * @JsonProperty 和 @JSONField 都是为了解决json字符串属性名和JavaBean的属性名匹配不上的问题
 *
 * access属性是用来控制是否能够被序列化或者反序列化 WRITE_ONLY只能反序列化
 *
 * Created by 凌战 on 2021/3/26
 */
public abstract class AbstractPageDto implements Serializable {

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public int pageNo;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public int pageSize;

    @JsonProperty(access = JsonProperty.Access.WRITE_ONLY)
    public int limit;

    public abstract String validate();



}
