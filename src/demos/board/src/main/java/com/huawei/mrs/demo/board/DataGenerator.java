package com.huawei.mrs.demo.board;

import org.apache.commons.cli.*;
import com.huawei.mrs.demo.board.util.MysqlParams;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class DataGenerator {
    private static final String INSERT_SQL = "INSERT INTO `demo`.`orders` VALUES (unix_timestamp(now(3)) * 1000, min(pow(rand(), -2), 10000), pow(rand(), -5), now())";

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException, ParseException {

        final Options options = new Options()
                .addOption(Option.builder("i")
                        .longOpt("average-interval")
                        .desc("Average interval seconds for insert one line of data")
                        .hasArg()
                        .build());
        final CommandLine cmd = new DefaultParser().parse(options, args);
        float interval = 1;
        if (cmd.hasOption("i")) {
            interval = Float.valueOf(cmd.getOptionValue("i"));
        }

        Class.forName(MysqlParams.DRIVER);
        final Connection connection = DriverManager.getConnection(MysqlParams.JDBC_URL, MysqlParams.USER, MysqlParams.PASSWORD);
        final Statement statement = connection.createStatement();
        final Random random = new Random();
        while (true) {
            try {
                statement.execute(INSERT_SQL);
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep((long) (random.nextFloat() * 2000 * interval));
        }
    }

}
