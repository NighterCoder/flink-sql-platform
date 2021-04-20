package com.flink.platform.web.utils;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * Spring上下文工具类
 * <p>
 * ApplicationContextAware让Spring容器传递自己生成的ApplicationContext给我们,
 * 然后我们把这个ApplicationContext设置成一个类的静态变量
 *
 * <p>
 * Created by 凌战 on 2021/4/15
 */
@Component
public class SpringContextUtils implements ApplicationContextAware {

    private static ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        SpringContextUtils.applicationContext = applicationContext;
    }

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }


    public static Object getBean(String beanName) {
        return applicationContext.getBean(beanName);
    }

    public static <T> T getBean(String beanName, Class<T> clazz) {
        return applicationContext.getBean(beanName, clazz);
    }

    public static <T> T getBean(Class<T> clazz) {
        return applicationContext.getBean(clazz);
    }

    public static Object getBean(String beanName, Object... args) throws BeansException {
        return applicationContext.getBean(beanName, args);
    }

    public static <T> T getBean(Class<T> clazz, Object... args) throws BeansException {
        return applicationContext.getBean(clazz, args);
    }

}
