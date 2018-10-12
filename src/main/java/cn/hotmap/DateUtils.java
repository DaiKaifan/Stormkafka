package cn.hotmap;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 时间解析工具类
 */
public class DateUtils {
    private DateUtils(){}

    private static DateUtils instance;

    public static DateUtils getInstance(){
        if(instance == null){
            instance = new DateUtils();
        }
        return  instance;
    }

    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public long getTime(String data)throws Exception {
        String time = data.substring(1,data.length()-1);
        Date date = simpleDateFormat.parse(time);
        return date.getTime();
    }

}
