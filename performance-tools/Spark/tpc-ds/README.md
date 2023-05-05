### 1.Compile the tpcds-ds/tpcds-gen project to obtain tpcds-gen-1.0-SNAPSHOT.jar
This simplifies creating tpc-ds data-sets on large scales on a hadoop cluster.
To get set up, you need to run 
$ make
this will download the TPC-DS dsgen program, compile it and use maven to build the MR app wrapped around it.
### 2. Ensure that tpcds-gen-1.0-SNAPSHOT.jar is stored in ~/tpcds-ds/tpcds-gen/target and dsdgen.jar is stored in target/lib.