HetuEngine tpc-ds性能测试工具使用方法：

一、工具部署：

    1、获取HetuEngine性能工具下载上传到客户端安装节点的/home/目录下。
        注：
        只能放到/home/目录下，脚本中有些目录是写死的

二、使用方法：

    1、使用hive客户端创建tpcds测试数据：

    1) 下载TPC-DS Benchmark工具
        本地下载，将ZIP文件上传到FusionInsight MRS集群客户端安装节点，并解压缩。具体步骤如下：

        下载tpc-ds工具。（下载链接：https://github.com/Altinity/tpc-ds/archive/refs/heads/master.zip）
        上传ZIP文件到FusionInsight MRS集群客户端安装节点。
        在FusionInsight MRS集群客户端安装节点解压缩上传的ZIP文件。
        unzip tpc-ds-master.zip

        在FusionInsight MRS集群客户端安装节点执行如下命令检测是否安装gcc，如果没有则需单独安装gcc。
        gcc -v

    2) 编译并生成TPC-DS数据
        切换到tpc-ds-master/bin目录
        cd tpc-ds-master/bin/

        编译数据生成器
        ./build-tool.sh

        将工具自带数据目录tpc-ds-master/data删除，避免数据目录重复存在
        rm -rf ../data

        生成数据（data scale = 1 (1GB of data)，数据格式为TEXT且以 | 作为内容分隔符）
        例如：./generate-data.sh 1

        切换到数据目录tpc-ds-master/data，并将生成的数据文件移动到对应tpc-ds表目录
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

    3) 加载TPC-DS数据
        切换到FusionInsight MRS客户端安装目录。例如：/opt/client
        cd /opt/client

        执行source Hive客户端，并kinit认证客户端用户（如果是安全模式集群）
        使用客户端用户在FusionInsight MRS集群HDFS创建数据目录。例如下，其中${SCALE}为TPC-DS数据规模
        hdfs dfs -mkdir -p /tmp/tpcds-generate/${SCALE}

        例如：hdfs dfs -mkdir -p /tmp/tpcds-generate/1

        切换到数据目录tpc-ds-master/data，上传本地TPC-DS数据到HDFS。例如下
        cd tpc-ds-master/data

        hdfs dfs -put ./* ${DIR}/${SCALE}/

        例如：hdfs dfs -put ./* /tmp/tpcds-generate/1

        使用Hive客户端创建TPC-DS数据表，其中${SCALE}为TPC-DS数据规模，${FORMAT}为TPC-DS数据格式，${temp_dir}为TPC-DS数据的HDFS路径，${database_path}为生成的TPC-DS表的存储路径

        sh run.sh ${SCALE} ${FORMAT} ${temp_dir} ${database_path}

        例如：sh /home/longrun/datainstall/run.sh 1 orc

    2、使用hetu客户端对数据进行analyze：

    1) 根据性能调优指导书对hetu集群进行性能调优;
    2) cd /home/longrun/datainstall;
    3) source 客户端，认证用户;
    4) sh analyze.sh ${catalog} ${schema}
        注：
        执行该脚本之前最好手动拉起Hetu的计算实例，不然有可能导致脚本中的某个analyze因首次提交任务集群拉起超时，导致执行失败。
        catalog：指定数据所在的catalog（使用共部署的话，catalog是hive）
        schema：执行数据库名称

    3、tpcds任务运行：

    1) vi /home/longrun/etc/longrun-static.cfg
        注：
        periodLenth=100000  #不用修改
        loopLenth=84000     #不用修改
        maxloopNum=1000     #表示执行轮次，tpcds语句103个sql全部跑完为一个轮次

    2) vi /home/longrun/longsql/hetu_tpcds/options.cfg
        注：
        instance=onequery   #不用修改
        pmaxNum=1           #并发数
        ifAlz=0             #不用修改
        batchInverval=10    #不用修改
        sqlInterval=3       #每个sql执行间隔
        waitInterval=10     #监控脚本等待刷新时间，不必修改
        biasThreshold=0.1   #偏差，不必修改
        workdir=/home/longrun/script    #不必修改
        pythondir=/home/longrun/script  #不必修改
        preconditionStr=echo <client password>  | source /opt/client/bigdata_env admintest; source /opt/client/bigdata_env;    #source客户端，认证用户，<client password> 需修改为真实的客户端认证密码
        prefix=use tpcds_orc_hive_1000;     #tpcds数据名称
        getconcurrent=ps -ef | grep "hetu-cli --tenant default --catalog hive --schema tpcds_orc_hive_1000 -f" | grep -v grep | grep -v "/bin/bash"   #命令行监控，使用该命令监控正在执行的sql，可手动执行测试，使用命令行执行一条sql时，执行该命令，查出一条记录即可，如果查询出来不是一条，可能导致并发参数失效
        runcmd=hetu-cli --tenant default --catalog hive --schema tpcds_orc_hive_1000 -f    #命令行

    3) cd /home/longrun/script;

    4) python longrunMain.py hetu_tpcds
        注：
        #使用该脚本启动tpcds任务，也可使用如下命令推到后台执行：
            nohup python longrunMain.py hetu_tpcds &
        #如果想终止任务，需要使用ps查询进程号，使用kill命令删除：
            ps -aux|grep "python l"|grep -v grep|awk '{print$2}'|xargs kill -9

    5) sh /home/longrun/longmon.sh hetu_tpcds
        注：
        #使用该脚本可实时监控任务执行情况
        #可使用ctrl+c中断
        #重复执行对任务没有影响

    6) cd /home/longrun/result;
        注：
        #hetu_tpcds目录下存放任务运行的结果
            onequery #存放监控显示的内容
            onequery-output #存放脚本运行的日志
                onequery-stat  #无需关注
            onequery-tmptask  #中间sql，无需关注
        #sh GetExcel.sh
            使用该脚本，会将任务运行结果整理收集到CSVfiles目录下的hetu_tpcds.csv文件中，可打开查看

        #重复执行任务的话，执行结果会被覆盖，如果想要保留上次执行的结果，需要手动备份。
