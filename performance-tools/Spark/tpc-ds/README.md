**Spark构造tpc-ds测试数据的方法**

运行要求：
    1.运行节点已安装Spark客户端；
    2.已创建访问Spark的用户；
    3.运行节点已安装gcc；
    4.执行过程先生成原始文件到本地，需要根据生成数据大小选用合适的磁盘。

操作步骤如下：
    1.下载TPC-DS Benchmark工具
        本地下载，将ZIP文件上传到FusionInsight MRS集群客户端安装节点，并解压缩。具体步骤如下：
        1）下载tpc-ds工具。（下载链接：https://github.com/Altinity/tpc-ds/archive/refs/heads/master.zip）

        2）上传ZIP文件到FusionInsight MRS集群客户端安装节点。
    
        3）在FusionInsight MRS集群客户端安装节点解压缩上传的ZIP文件。
        unzip tpc-ds-master.zip
    
        注意：在FusionInsight MRS集群客户端安装节点执行如下命令检测是否安装gcc，如果没有则需单独安装gcc。
        gcc -v
        如果当前集群节点的gcc版本不是4.8.5，则需要找一个安装4.8.5版本的gcc的节点执行下述第2步操作中编译数据生成器，将编译成功后的工具包拷贝到客户端安装节点再执行第2步操作中的4）继续生成数据。
    
    2.编译并生成TPC-DS数据
        1）切换到tpc-ds-master/bin目录
        cd tpc-ds-master/bin/
    
        2）编译数据生成器
        ./build-tool.sh
    
        4）将工具自带数据目录tpc-ds-master/data删除，避免数据目录重复存在
        rm -rf ../data
    
        5）生成数据（data scale = 1 (1GB of data)，数据格式为TEXT且以 | 作为内容分隔符）
        例如：./generate-data.sh 1
    
        6）切换到数据目录tpc-ds-master/data，并将生成的数据文件移动到对应tpc-ds表目录
        cd tpc-ds-master/data
        mkdir call_center
        mkdir catalog_page
        mkdir catalog_returns
        mkdir catalog_sales
        mkdir customer
        mkdir customer_address
        mkdir customer_demographics
        mkdir date_dim
        mkdir dbgen_version
        mkdir household_demographics
        mkdir income_band
        mkdir inventory
        mkdir item
        mkdir promotion
        mkdir reason
        mkdir ship_mode
        mkdir store
        mkdir store_returns
        mkdir store_sales
        mkdir time_dim
        mkdir warehouse
        mkdir web_page
        mkdir web_returns
        mkdir web_sales
        mkdir web_site
        mv call_center*.dat ./call_center
        mv catalog_page*.dat ./catalog_page
        mv catalog_returns*.dat ./catalog_returns
        mv catalog_sales*.dat ./catalog_sales
        mv customer_demographics*.dat ./customer_demographics
        mv customer_address*.dat ./customer_address
        mv customer*.dat ./customer
        mv date_dim*.dat ./date_dim
        mv dbgen_version*.dat ./dbgen_version
        mv household_demographics*.dat ./household_demographics
        mv income_band*.dat ./income_band
        mv inventory*.dat ./inventory
        mv item*.dat ./item
        mv promotion*.dat ./promotion
        mv reason*.dat ./reason
        mv ship_mode*.dat ./ship_mode
        mv store_returns*.dat ./store_returns
        mv store_sales*.dat ./store_sales
        mv store*.dat ./store
        mv time_dim*.dat ./time_dim
        mv warehouse*.dat ./warehouse
        mv web_page*.dat ./web_page
        mv web_returns*.dat ./web_returns
        mv web_sales*.dat ./web_sales
        mv web_site*.dat ./web_site
    
    3.加载TPC-DS数据
        1) 切换到FusionInsight MRS客户端安装目录。例如：/opt/client
        cd /opt/client
    
        2) 执行source Spark客户端，并kinit认证客户端用户（如果是安全模式集群）
    
        3) 使用客户端用户在FusionInsight MRS集群HDFS创建数据目录，若为存算分离场景，改成OBS目录。例如下，其中${SCALE}为TPC-DS数据规模
        hdfs dfs -mkdir -p /tmp/tpcds/${SCALE}
        例如：hdfs dfs -mkdir -p /tmp/tpcds/1
    
        4) 切换到数据目录tpc-ds-master/data，上传本地TPC-DS数据到HDFS。例如下
        cd tpc-ds-master/data
        hdfs dfs -put ./* ${DIR}/${SCALE}/
        例如：hdfs dfs -put ./* /tmp/tpcds/1
