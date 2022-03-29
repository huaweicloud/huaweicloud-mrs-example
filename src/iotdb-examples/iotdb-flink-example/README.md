# IoTDB-Flink-Connector Example

## Function
```
The example is to show how to send data to a IoTDB server from a Flink job.
```

## Usage

* Launch the IoTDB server.
* Run `org.apache.iotdb.flink.FlinkIoTDBSink.java` to run the flink job on local mini cluster.

# TsFile-Flink-Connector Example

## Usage

* Run `org.apache.iotdb.flink.FlinkTsFileBatchSource.java` to create a tsfile and read it via a flink DataSet job on local mini cluster.
* Run `org.apache.iotdb.flink.FlinkTsFileStreamSource.java` to create a tsfile and read it via a flink DataStream job on local mini cluster.
* Run `org.apache.iotdb.flink.FlinkTsFileBatchSink.java` to write a tsfile via a flink DataSet job on local mini cluster and print the content to stdout.
* Run `org.apache.iotdb.flink.FlinkTsFileStreamSink.java` to write a tsfile via a flink DataStream job on local mini cluster and print the content to stdout.
