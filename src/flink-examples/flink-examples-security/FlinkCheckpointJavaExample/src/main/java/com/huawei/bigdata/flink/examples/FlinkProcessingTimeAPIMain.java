package com.huawei.bigdata.flink.examples;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class FlinkProcessingTimeAPIMain {
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        System.out.println("use command as: ");
        System.out.println(
                "./bin/flink run --class com.huawei.bigdata.flink.examples.FlinkProcessingTimeAPIMain"
                        + "<path of FlinkCheckpointJavaExample jar> --chkPath <checkpoint path>");
        System.out.println("**************************************************************************************");
        System.out.println("checkpoint path should be start with hdfs:// or file://");
        System.out.println("***************************************************************************************");

        ParameterTool paraTool = ParameterTool.fromArgs(args);
        final String chkPath = paraTool.get("chkPath");
        if (chkPath == null) {
            System.out.println("NO checkpoint path is given!");
            System.exit(1);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStateBackend(new FsStateBackend(chkPath));
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointInterval(6000);

        env.addSource(new SEventSourceWithChk())
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(4), Time.seconds(1)))
                .apply(new WindowStatisticWithChk())
                .print();

        env.execute();
    }
}
