# CTBase导出数据样例

样例基本功能：使用spark2x批量导出ctbase用户表数据。

支持的场景：

* 非独立索引表/独立索引表，主索引/二级索引导出，带条件导出。

限制说明：
1.导出条件仅支持索引字段，与索引同时使用时有效，否则请增加后置过滤进行数据筛选。
2.本样例采用命令提交方式执行，仅支持在Linux环境上执行，需要本地编包后，上传jar包到Linux客户端节点执行。

## 环境准备

* 支持Scala开发的IDE(例如IDEA+Scala插件)
* maven工具
* MRS客户端环境

## 注意事项

1. 目前CTBase相关类暂不支持序列化，无法在spark算子中使用，建议在客户端/Driver侧进行转换，但是需要注意一次扫描获取的数据量不能过大，否则可能引发OOM问题，建议分批扫描导出，例如采用给定扫描数据的时间范围等方式。
2. 目前MRS-Spark使用的Scala版本为2.12.19

## 数据准备

1. 登录客户端节点，执行 **source 客户端目录/bigdata_env**，进行账户认证：安全环境，执行**kinit *用户名***;非安全环境执行**export HADOOP_USER_NAME=*用户名***。
2. 执行**ctbase shell**进入ctbase客户端。
3. 执行以下步骤生成样例数据。

```
#非独立索引表
#创建表
create_clustertable 'ClusterTableTest',{NAME=>'F',COMPRESSION=>'SNAPPY'},METADATA=>{'CTBASE_ROWKEY_SEPARATOR'=>'^','INDEX_TABLE_STANDALONE' => 'false'}
create_usertable 'ClusterTableTest','Consumer_info'
create_usertable 'ClusterTableTest','Account_info'
create_usertable 'ClusterTableTest','User_info'

#数据定义
add_columns 'ClusterTableTest','Consumer_info','F',{COLUMNS=>['ID:BIGINT:10:a','UID:VARCHAR:10:a','NAME:CHAR:20:b','AGE:INT:3:c','WEIGHT:FLOAT:10:d','HIGHT:DOUBLE:20:d','LOCATION:DECIMAL:30:d','GENDER:BOOL:1:e','CDATE:VARCHAR:10:f','UDATE:CHAR:20:f']}
add_columns 'ClusterTableTest','Account_info','F',{COLUMNS=>['ID:BIGINT:10:a','UID:VARCHAR:10:a','NAME:CHAR:20:b','AGE:INT:3:c','WEIGHT:FLOAT:10:d','HIGHT:DOUBLE:20:d','LOCATION:DECIMAL:30:d','GENDER:BOOL:1:e','CDATE:VARCHAR:10:f','UDATE:CHAR:20:f']}
add_columns 'ClusterTableTest','User_info','F',{COLUMNS=>['ID:BIGINT:10:a','UID:VARCHAR:10:a','NAME:CHAR:20:b','AGE:INT:3:c','WEIGHT:FLOAT:10:d','HIGHT:DOUBLE:20:d','LOCATION:DECIMAL:30:d','GENDER:BOOL:1:e','CDATE:VARCHAR:10:f','UDATE:CHAR:20:f']}

#创建主索引
create_index 'ClusterTableTest','Consumer_info','idx_p1',{PRIMARY=>true,KEY_COLUMN_INDEPENDENT=>false,SECTIONS=>['ID'],SPLITS=>'1,2,3,4'}
create_index 'ClusterTableTest','Account_info','idx_p2',{PRIMARY=>true,KEY_COLUMN_INDEPENDENT=>false,SECTIONS=>['ID'],SPLITS=>'1,2,3,4,'},CLUSTER=>{USERTABLE=>'Consumer_info',INDEX =>'idx_p1'}
create_index 'ClusterTableTest','User_info','idx_p3',{PRIMARY=>true,KEY_COLUMN_INDEPENDENT=>false,SECTIONS=>['ID'],SPLITS=>'1,2,3,4'}

#创建二级索引
create_index 'ClusterTableTest','Consumer_info','idx_s1',{PRIMARY=>false,OPTIONAL=>true,SECTIONS=>['NAME'],SPLITS=>'1,2,3,4'}
create_index 'ClusterTableTest','Account_info','idx_s2',{PRIMARY=>false,OPTIONAL=>true,SECTIONS=>['NAME','AGE'],SPLITS=>'1,2,3,4'}
create_index 'ClusterTableTest','User_info','idx_s3',{PRIMARY=>false,OPTIONAL=>true,SECTIONS=>['NAME','UID'],SPLITS=>'1,2,3,4'}


#写入数据
for i in 1..201
    put 'ClusterTableTest','Consumer_info',CTROW=>{'ID'=>'1000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'zhangsan'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>rand.to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s}

    put 'ClusterTableTest','Consumer_info',CTROW=>{'ID'=>'2000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'lisi'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>(0.5-rand()).to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s}

    put 'ClusterTableTest','Consumer_info',CTROW=>{'ID'=>'3000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'wangwu'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>(0.5-rand()).to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s}

    put 'ClusterTableTest','Account_info',CTROW=>{'ID'=>'1000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'lilei'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>(0.5-rand()).to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s}

    put 'ClusterTableTest','Account_info',CTROW=>{'ID'=>'2000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'hanmeimei'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>(0.5-rand()).to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s}

    put 'ClusterTableTest','Account_info',CTROW=>{'ID'=>'1000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'Bob'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>(0.5-rand()).to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s}
end

#独立索引表
create_clustertable 'ClusterTableTestStandAlone',{NAME=>'F',COMPRESSION=>'SNAPPY'},METADATA=>{'CTBASE_ROWKEY_SEPARATOR'=>'^','INDEX_TABLE_STANDALONE' => 'true'}
create_usertable 'ClusterTableTestStandAlone','Consumer_info_StandAlone'
#数据定义
add_columns 'ClusterTableTestStandAlone','Consumer_info_StandAlone','F',{COLUMNS=>['ID:BIGINT:10:a','UID:VARCHAR:10:a','NAME:CHAR:20:b','AGE:INT:3:c','WEIGHT:FLOAT:10:d','HIGHT:DOUBLE:20:d','LOCATION:DECIMAL:30:d','GENDER:BOOL:1:e','CDATE:VARCHAR:10:f','UDATE:CHAR:20:f','ISCHINESE:BOOL:1:g']}
#创建主索引
create_index 'ClusterTableTestStandAlone','Consumer_info_StandAlone','idx_p4',{PRIMARY=>true,KEY_COLUMN_INDEPENDENT=>false,SECTIONS=>['ID'],SPLITS=>'1,2,3,4'}
#创建二级索引
create_index 'ClusterTableTestStandAlone','Consumer_info_StandAlone','idx_s4',{PRIMARY=>false,OPTIONAL=>true,SECTIONS=>['ISCHINESE'],SPLITS=>'0,1'}
#写入数据
for i in 1..201
    put 'ClusterTableTestStandAlone','Consumer_info_StandAlone',CTROW=>{'ID'=>'1000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'Ming'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>rand.to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s,'ISCHINESE'=>rand(2).to_s}
  
    put 'ClusterTableTestStandAlone','Consumer_info_StandAlone',CTROW=>{'ID'=>'2000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'Zhao'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>rand.to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s,'ISCHINESE'=>rand(2).to_s}

    put 'ClusterTableTestStandAlone','Consumer_info_StandAlone',CTROW=>{'ID'=>'3000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'Liu'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>rand.to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s,'ISCHINESE'=>rand(2).to_s}

    put 'ClusterTableTestStandAlone','Consumer_info_StandAlone',CTROW=>{'ID'=>'4000'+i.to_s,'UID'=>rand(1000).to_s,'NAME'=>'Li'+i.to_s,'AGE'=>rand(150).to_s,'WEIGHT'=>(100*rand).to_s,'HIGHT'=>(200*rand).to_s,'LOCATION'=>rand.to_s,'GENDER'=>rand(2).to_s,'CDATE'=>'2021120'+rand(10).to_s,'UDATE'=>'2021121'+rand(10).to_s,'ISCHINESE'=>rand(2).to_s}
end
```

## 参数调整

所有参数定义在com.huawei.bigdata.ctbase.examples.CTBaseExportExample中。

* PARTITION_NUM
  spark RDD 分区参数，用于hbase数据扫描以及数据存储过程，影响导出性能，请根据环境进行调整。
* CACHE_NUM
  HBase scan缓存数量，用于hbase数据扫描过程，影响导出性能，请根据环境进行调整。
* DELETE_PATH_FLAG
  是否删除上次导出的路径，建议改为false，避免覆盖上次导出数据，仅测试时可改为true。
* queryColumns
  导出列，可在初始化过程时自定义。

## 导出步骤

本样例是ctbase样例的一部分，使用时，需要打开ctbase样例工程的下面一级目录：sample_project/src/ctbase-examples/ctbase-export-examples。

1. 打开ctbase导出样例工程，执行**mvn clean package**编译项目。
2. 拷贝target目录下最内层的ctbase-export-examples-8.5.1-351.r22目录到客户端节点任意位置，例如/opt。
3. 登录客户端节点，执行**cd /opt**进入到该目录下，执行**chmod -R 755 ctbase-export-examples-8.5.1-351.r22**，给予权限。
4. 执行 **source 客户端目录/bigdata_env**，进行账户认证：安全环境，执行**kinit *用户名***;非安全环境执行**export HADOOP_USER_NAME=*用户名***。
5. 执行**cd ctbase-export-examples-8.5.1-351.r22**进入脚本目录下，执行任务提交脚本，**sh submit.sh**, 参数传入说明见脚本注释。
   注意：如果脚本遇到换行符问题无法执行，请切换为LF格式。
6. 执行命令，拷贝HDFS下的导出结果到本地路径下，例如存到/tmp/export目录下，查看本地路径下导出的数据。

```
#创建本地目录
[ ! -d "/tmp/export" ] && mkdir -p /tmp/export
#拷贝
#拷贝非独立索引表+主索引导出结果
hdfs dfs -get /tmp/exportByPriIdx /tmp/export
#拷贝非独立索引表+主索引+条件导出结果
hdfs dfs -get /tmp/exportByPriIdxWithCond /tmp/export
#拷贝非独立索引表+二级索引导出结果
hdfs dfs -get /tmp/exportBySecIdx /tmp/export
#拷贝非独立索引表+二级索引+条件导出结果
hdfs dfs -get /tmp/exportBySecIdxWithCond /tmp/export
#拷贝独立索引表+二级索引导出结果
hdfs dfs -get /tmp/exportByPriIdxSA /tmp/export
#拷贝独立索引表+二级索引+条件导出结果
hdfs dfs -get /tmp/exportBySecIdxWithCondSA /tmp/export
```

