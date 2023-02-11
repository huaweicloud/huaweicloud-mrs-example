import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("HudiPythonExample").getOrCreate()

import configparser

config_name=sys.argv[1]

config = configparser.ConfigParser()
config.read(str(config_name))

outputDirectory=config['job']['outputDirectory']
save_path=config['write_properties']['save_path']
save_mode=config['write_properties']['save_mode']
table_properties = config['table_properties']

hudi_options={}

for i in table_properties:
  hudi_options[i]=config['table_properties'][i]

df = spark.read.format("ORC").load(str(outputDirectory) + "out_hive/*")

df.write.format("hudi").options(**hudi_options).mode(save_mode).save(save_path)

spark.close()