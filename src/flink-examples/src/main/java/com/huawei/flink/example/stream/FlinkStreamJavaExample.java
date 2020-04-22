package com.huawei.flink.example.stream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkStreamJavaExample {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkStreamJavaExample.class);

    public static void main(String[] args) throws  Exception
    {
        System.out.println("use command as: ");
        System.out.println("./bin/flink run --class com.huawei.flink.example.stream.FlinkStreamJavaExample /opt/flink-examples-1.0.jar --filePath /opt/log1.txt,/opt/log2.txt --windowTime 2");
        System.out.println("******************************************************************************************");
        System.out.println("<filePath> is for text file to read data, use comma to separate");
        System.out.println("<windowTime> is the width of the window, time as minutes");
        System.out.println("******************************************************************************************");

        // input test file
        final String[] filePaths = ParameterTool.fromArgs(args).get("filePath", "/opt/log1.txt,/opt/log2.txt").split(",");

        // windows time
        final int windowTime = ParameterTool.fromArgs(args).getInt("windowTime", 120000);

        final int shoppingTimeTh = ParameterTool.fromArgs(args).getInt("shoppingTime", 120);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> text = env.readTextFile(filePaths[0]);
        for (String filePath : filePaths) {
            if (filePath.equals(filePaths[0])) {
                continue;
            }
            text = text.union(env.readTextFile(filePath));
        }

        text.map(new MapFunction<String, UserRecord>() {
                    @Override
                    public UserRecord map(String s) throws Exception
                    {
                        return getRecord(s);
                    }
                })
                .assignTimestampsAndWatermarks(new Record2TimestampExtractor())
                .filter(new FilterFunction<UserRecord>() {
                    @Override
                    public boolean filter(UserRecord userRecord) throws Exception
                    {
                        LOG.info("the first filter input is: " + userRecord);
                        return userRecord.sexy.equals("female");
                    }
                })
                .keyBy(new UserRecordSelector())
                .window(TumblingEventTimeWindows.of(Time.seconds(windowTime)))
                .reduce(new ReduceFunction<UserRecord>() {
                    @Override
                    public UserRecord reduce(UserRecord userRecord1, UserRecord userRecord2) throws Exception
                    {
                        userRecord1.shoppingTime += userRecord2.shoppingTime;
                        LOG.info("after reduce output is: " + userRecord1);
                        return userRecord1;
                    }
                })
                .filter(new FilterFunction<UserRecord>() {
                    @Override
                    public boolean filter(UserRecord userRecord) throws Exception
                    {
                        LOG.info("the Last filter input is: " + userRecord);
                        return userRecord.shoppingTime > shoppingTimeTh;
                    }
                })
                .print();

        env.execute("FemaleInfoCollectionPrint java");
    }

    private static UserRecord getRecord(String line) {
        String[] elems = line.split(",");
        assert elems.length == 3;
        UserRecord userRecord = new UserRecord(elems[0], elems[1], Integer.parseInt(elems[2]));
        LOG.info("userRecord is: " + userRecord);
        return userRecord;
    }

    // 构造继承AssignerWithPunctuatedWatermarks的类，用于设置eventTime以及waterMark
    private static class Record2TimestampExtractor implements AssignerWithPunctuatedWatermarks<UserRecord> {

        // add tag in the data of datastream elements
        @Override
        public long extractTimestamp(UserRecord element, long previousTimestamp) {
            return System.currentTimeMillis();
        }

        // give the watermark to trigger the window to execute, and use the value to check if the window elements is ready
        @Override
        public Watermark checkAndGetNextWatermark(UserRecord element, long extractedTimestamp) {
            LOG.info("element " + element + ", extractedTimestamp is: " + extractedTimestamp);
            LOG.info("element " + element + ", extractedTimestamp - 1 is: " + (extractedTimestamp - 1));
            return new Watermark(extractedTimestamp - 1);
        }
    }

    // 构造keyBy的关键字作为分组依据
    private static class UserRecordSelector implements KeySelector<UserRecord, Tuple2<String, String>> {
        @Override
        public Tuple2<String, String> getKey(UserRecord value) throws Exception {
            LOG.info("the second filter input is: " + value);
            return Tuple2.of(value.name, value.sexy);
        }
    }

    // UserRecord数据结构的定义，并重写了toString打印方法
    public static class UserRecord {
        private String name;
        private String sexy;
        private int shoppingTime;

        public UserRecord(String n, String s, int t) {
            name = n;
            sexy = s;
            shoppingTime = t;
        }

        public String toString() {
            return "name: " + name + "  sexy: " + sexy + "  shoppingTime: " + shoppingTime;
        }
    }
}
