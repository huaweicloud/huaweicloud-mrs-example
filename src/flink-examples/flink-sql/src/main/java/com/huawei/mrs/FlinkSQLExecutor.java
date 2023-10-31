package com.huawei.mrs;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.util.Locale;

/**
 * SQL 作业主函数提交入口
 */
public class FlinkSQLExecutor {
    public static void main(String[] args) throws IOException {
        System.out.println("--------------------  begin init ----------------------");
        final String sqlPath = ParameterTool.fromArgs(args).get("sql", "config/redisSink.sql");
        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings);
        StatementSet statementSet = tableEnv.createStatementSet();
        String sqlStr = FileUtils.readFileToString(FileUtils.getFile(sqlPath), "utf-8");
        String[] sqlArr = sqlStr.split(";");
        for (String sql : sqlArr) {
            sql = sql.trim();
           if (sql.toLowerCase(Locale.ROOT).startsWith("create")) {
               System.out.println("----------------------------------------------\nexecuteSql=\n" + sql);
               tableEnv.executeSql(sql);
           } else if (sql.toLowerCase(Locale.ROOT).startsWith("insert")) {
               System.out.println("----------------------------------------------\ninsert=\n" + sql);
               statementSet.addInsertSql(sql);
           }
        }
        System.out.println("---------------------- begin exec sql --------------------------");
        statementSet.execute();
    }
}
