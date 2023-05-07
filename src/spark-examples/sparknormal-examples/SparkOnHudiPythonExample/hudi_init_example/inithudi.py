import sys
import configparser
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HudiPythonExample").getOrCreate()

config_name=sys.argv[1]

config = configparser.ConfigParser()
config.read(str(config_name))
save_path=config['write_properties']['save_path']
save_mode=config['write_properties']['save_mode']
table_properties = config['table_properties']

hudi_options={}

for i in table_properties:
  hudi_options[i]=config['table_properties'][i]
#读取写入数据的dataframe，根据实际场景来修改读取文件格式和路径
base_data = spark.read.parquet("/tmp/tb_base")
#写入Hudi表
base_data.write.format("hudi").options(**hudi_options).mode(save_mode).save(save_path)
