Compilation Guide
==============
You can run the following mvn command to compile module xsql-data-generator:
    mvn clean package

Usage Guide
========
You can run the following command to generate 100,000 data records:
    spark-submit --master yarn-client --class com.hibench.XSQLDataGenerator --driver-memory 1G --executor-memory 1G --executor-cores 1 --num-executors 8 --conf spark.yarn.executor.memoryOverhead=1024 /opt/hudi/XSQLDataGenerator.jar 10G "int(2)|string(2)|int(4)|string(4)|double(5:3)|Date(3M)|timestamp(1Y)|int(3)|int(5)|int(6)|int(7)|int(7)|int(6)|int(5)|string(6)|string(8)|string(10)|double(3:2)|double(4:2)|Date(1M)|Date(2M)|Date(4M)|Date(5M)|Date(7M)|Date(8M)|Date(9M)|Date(1Y)|timestamp(10Y)|int(4)|string(15)|double(7:3)|Date(6M)|timestamp(6Y)|int(4)|string(15)|double(5:3)|Date(3M)|timestamp(1Y)|int(4)|string(15)|double(5:3)|Date(3M)|timestamp(1Y)|int(4)|string(15)|double(5:3)|Date(3M)|timestamp(1Y)|int(3)|string(25)|string(24)|int(4)|double(5:3)|timestamp(3M)|string(27)|int(3)|string(25)|string(26)|int(5)|double(5:3)|timestamp(6M)|string(27)|int(3)|string(25)|string(26)|int(4)|double(5:3)|timestamp(6M)|string(25)|int(6)|string(20)|int(5)|string(15)|string(20)|int(5)|double(5:3)|Date(3M)|timestamp(3M)|int(5)|string(15)|string(20)|int(5)|double(5:3)|Date(3M)|timestamp(3Y)|int(5)|string(15)|string(20)|int(5)|double(5:3)|Date(3M)|timestamp(3Y)|string(20)|string(21)|string(22)|string(23)|string(24)|string(25)|string(26)|string(27)|string(18)|string(19)" /tmp/upsertData_100m 80 100000

1. Upload XSQLDataGenerator.jar to a specified path, for example, "/opt/hudi/".
2. The number and type of fields need to be specified like "int(2)|string(2)|int(4)".
3. The generated data is stored in the specified HDFS directory, for example, "/tmp/upsertData_100m".
4. Most importantly, we need to specify the number of rows of data and how many files the data will be stored in, for example, 100000 rows of data are stored in 80 files.