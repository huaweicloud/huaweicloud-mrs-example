package com.huawei.bigdata.spark.examples;

import com.huawei.hadoop.security.LoginUtil;

import org.apache.spark.sql.SparkSession;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class HBaseExternalHivetoCarbon {
    public final static long TIMEWINDOW = 30 * 60 * 1000;//synchronization interval，default:30 mins

    public static long timeStart = System.currentTimeMillis();

    public static long timeEnd;

    public static SparkSession spark;

    public static String queryTimeStart;

    public static String queryTimeEnd;

    public static String carbonTableName = "carbon01";

    public static String externalHiveTableName = "external_hbase_table";

    public static StringBuilder cmdsb;

    public static String transferDateToStr(long time) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(new Date(time));
    }

    public static void main(String[] args) throws Exception {
        String userPrincipal = "sparkuser";
        String userKeytabPath = "/opt/FIclient/user.keytab";
        String krb5ConfPath = "/opt/FIclient/KrbClient/kerberos/var/krb5kdc/krb5.conf";
        Configuration hadoopConf = new Configuration();
        LoginUtil.login(userPrincipal, userKeytabPath, krb5ConfPath, hadoopConf);

        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            public void run() {
                spark = SparkSession.builder().appName("HBaseExternalHiveToCarbon").getOrCreate();

                timeEnd = timeStart + TIMEWINDOW;

                queryTimeStart = transferDateToStr(timeStart);
                queryTimeEnd = transferDateToStr(timeEnd);

                //run delete logic
                cmdsb = new StringBuilder();
                cmdsb.append("delete from ")
                    .append(carbonTableName)
                    .append("  where key in (select key from ")
                    .append(externalHiveTableName)
                    .append(" where modify_time>'")
                    .append(queryTimeStart)
                    .append("' and modify_time<'")
                    .append(queryTimeEnd)
                    .append("' and valid='0')");
                spark.sql(cmdsb.toString());

                //run insert logic
                cmdsb = new StringBuilder();
                cmdsb.append("insert into ")
                    .append(carbonTableName)
                    .append("  select * from ")
                    .append(externalHiveTableName)
                    .append(" where modify_time>'")
                    .append(queryTimeStart)
                    .append("'  and modify_time<'")
                    .append(queryTimeEnd)
                    .append("'  and valid='1'");
                spark.sql(cmdsb.toString());

                timeStart = timeEnd;
            }
        }, TIMEWINDOW, TIMEWINDOW);
    }
}
