生成ClickHouse性能测试工具Star Schema Benchmark
1. 下载性能测试工具源代码 <https://github.com/vadimtk/ssb-dbgen/archive/refs/heads/master.zip>
2. 上传到linux机器，解压源代码
````
unzip ssb-dbgen-master.zip -d ssb-dbgen
````
3. 准备编译环境，安装make工具
4. 编译性能工具
````
cd ssb-dbgen/ssb-dbgen-master
make
````