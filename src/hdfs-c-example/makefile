CFLAGS += -I${HADOOP_HOME}/include
CFLAGS += -Wall
LDFLAGS += -lhdfs

all: hdfs_test

hdfs_test.o: hdfs_test.c
	$(CC) -c $(CFLAGS) -o $@ $<

hdfs_test: hdfs_test.o
	$(CC) -o $@ $^ $(LDFLAGS) 

clean:
	-rm -f hdfs_test.o
	-rm -f hdfs_test
	
run:
	./hdfs_test

gdb:
	-gdb hdfs_test
