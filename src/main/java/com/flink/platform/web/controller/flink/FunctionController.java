package com.flink.platform.web.controller.flink;


import com.flink.platform.web.common.entity.Msg;
import com.flink.platform.web.controller.BaseController;
import com.flink.platform.web.service.FunctionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

/**
 * 自定义函数注册 Controller
 *
 * todo 当前思路是先将udf jar包上传至hdfs上,当在实际执行flink sql初始化环境时,再去下载jar包至本地,最后在本地jobGraph构造时add,随jobGraph提交至集群
 * todo 经历了 本地 -> hdfs -> 本地 -> hdfs 待优化
 *
 * <p>
 * 主要用于flink sql下动态支持自定义函数注册：
 * [
 * 1.上传jar包
 * 2.调用registerFunctions函数
 * ]
 * <p>
 * Created by 凌战 on 2021/3/1
 */
@RestController
@RequestMapping("/api/v1/function")
@Slf4j
public class FunctionController extends BaseController {



    @Autowired
    private FunctionService functionService;


    /**
     * 用于flink sql场景自定义函数注册使用
     * 上传jar包并且注册成方法
     * <p>
     * PS:
     * 1. 这里只会将相关信息保存在数据库中,并且会上传jar包至hdfs;
     * 2. 在flink sql执行环境初始化时,会访问相关数据库获取信息,与配置文件中的静态自定义方法一起注册;
     * 3. 具体在ExecutionContext中的registerFunctions中
     *
     * @param functionName 函数名
     * @param className    类名
     * @param files        文件
     * @return void
     */
    @PutMapping
    public Msg put(@RequestParam("functionName") String functionName,
                   @RequestParam("className") String className,
                   @RequestParam("files") MultipartFile[] files) {
        try{
            functionService.save(functionName,className,files);
        }catch (Exception e){
            log.error("upload error!", e);
            throw new RuntimeException("上传出错");
        }
        return success();
    }


}
