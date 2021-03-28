package com.flink.platform.web.controller;

import com.flink.platform.web.common.entity.Msg;
import com.flink.platform.web.common.entity.login.LoginUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.servlet.http.HttpServletRequest;

public abstract class BaseController {

    @Autowired
    private HttpServletRequest request;


    protected LoginUser getCurrentUser() {
        return (LoginUser) SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    }


    protected Msg success() {
        return success(null);
    }

    protected Msg success(Object content) {
        return success("操作成功", content);
    }

    protected Msg success(String msg, Object content) {
        return Msg.create(0, msg, content);
    }

    protected Msg failed() {
        return failed("操作失败");
    }

    protected Msg failed(String msg) {
        return failed(-1, msg);
    }

    protected Msg failed(int code, String msg) {
        return Msg.create(code, msg, null);
    }


}
