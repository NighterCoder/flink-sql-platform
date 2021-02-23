package com.flink.platform.web.common.util;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by 凌战 on 2021/2/22
 */
public class DateUtils {

    public static final String DATE_FORMAT="yyyy-MM-dd";
    public static final String DATE_TIME_FORMAT="yyyy-MM-dd HH:mm:ss";



    public static String format(LocalDateTime date, String format) {
        return date.format(DateTimeFormatter.ofPattern(format));
    }





}
