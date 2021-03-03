package com.flink.platform.core;

import com.flink.platform.core.config.UDFRegister;
import com.flink.platform.core.config.entries.DeerEntry;
import com.flink.platform.core.exception.SqlPlatformException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by 凌战 on 2021/3/3
 */
public class DeerHelper {

    private static final String UDF_SQL_PATH = "query_udf.sql";

    private DeerEntry deerEntry;

    public DeerHelper(DeerEntry entry) {
        this.deerEntry = entry;
    }


    /**
     * 查询注册的Udf jar包
     * @return List<UDFRegister>
     */
    public List<UDFRegister> queryUdfRegisters(){
        List<UDFRegister> udfs = new ArrayList<>();

        try (Connection conn = DriverManager.getConnection(deerEntry.getDeerUrl(), deerEntry.getDeerUsername(), deerEntry.getDeerPassword())) {
            PreparedStatement stmt = conn.prepareStatement(StringUtils.join(read(UDF_SQL_PATH)," "));
            stmt.executeQuery();
            ResultSet rs = stmt.getResultSet();

            while(rs.next()){
                UDFRegister register = new UDFRegister();
                register.setFunctionName(rs.getString("function_name"));
                register.setClassName(rs.getString("class_name"));
                register.setJarName(rs.getString("jar_name"));
                udfs.add(register);
            }
        }catch (SQLException e) {
            throw new SqlPlatformException("连接数据库异常,请联系后台人员",e);
        }
        return udfs;
    }




    private List<String> read(String fileName){
        try (InputStream in = DeerHelper.class.getClassLoader().getResourceAsStream("sql/"+fileName)){
            assert in != null;
            return IOUtils.readLines(in);
        } catch (IOException e) {
            throw new SqlPlatformException("读取配置异常", e);
        }
    }


}
