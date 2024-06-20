import os
import sys
import configparser

config_name=sys.argv[1]

config = configparser.ConfigParser()
config.read(str(config_name))

connectionName=config['connection']['connectionName']
connectionString=config['connection']['connectionString']
jdbcDriver=config['connection']['jdbcDriver']
username=config['connection']['username']
# There are security risks when storing passwords in plain text. It is recommended to store the password in cipher text in the configuration file or environment variable and decrypt it when used to ensure security.
password=config['connection']['password']

jobName=config['job']['jobName']
schemaName=config['job']['schemaName']
tableName=config['job']['tableName']
outputDirectory=config['job']['outputDirectory']
trans=config['job']['trans']

precombineFiled=config['hudi']['precombineFiled']
recordkeyFiled=config['hudi']['recordkeyFiled']
table_type=config['hudi']['table_type']
keygenerator=config['hudi']['keygenerator']
writeOperation=config['hudi']['writeOperation']
extractor=config['hudi']['extractor']
parallelism=config['hudi']['parallelism']
writeMode=config['hudi']['writeMode']
partitionFileds=config['hudi']['partitionFileds']


#connect_result=os.popen("/opt/newClient/Loader/loader-tools-1.99.3/sqoop-shell/sqoop2-shell -c 'create connection -c 1 --help'").readlines()

connect_result=os.popen("/opt/newClient/Loader/loader-tools-1.99.3/sqoop-shell/sqoop2-shell -c 'create connection -c 1 -n " + str(connectionName) + " --connector-connection-connectionString " + str(connectionString) + " --connector-connection-jdbcDriver " + str(jdbcDriver) + " --connector-connection-username " + str(username) + " --connector-connection-password " + str(password) + "'").readlines()
connect_id=""
if "FINE" in connect_result[len(connect_result)-1]:
  connect_msg=connect_result[len(connect_result)-2].split(' ')
  connect_id=str(connect_msg[len(connect_msg)-1]).replace('\n', '')
if "" == connect_id:
  print("create connection faild")
  for i in connect_result:
    print(i)
  os._exit(1)
else:
  print("create connection success, connect_id is: " + connect_id)
  
job_id=""
job_result=os.popen("/opt/newClient/Loader/loader-tools-1.99.3/sqoop-shell/sqoop2-shell -c 'create job -x " + connect_id + " -t import --name " + str(jobName) + " --connector-table-schemaName " + str(schemaName) + " --connector-table-tableName " + str(tableName) + " --connector-table-needPartition false --framework-output-storageType HIVE --framework-output-outputDirectory " + str(outputDirectory) + "out_hive/" + " --framework-throttling-extractors 1 --trans " + str(trans) + "'").readlines()
if "FINE" in job_result[len(job_result)-1]:
  job_msg=job_result[len(job_result)-2].split(' ')
  job_id=job_msg[len(job_msg)-1].replace('\n', '')
if "" == job_id:
  print("create job faild")
  for i in job_result:
    print(i)
  os._exit(1)
else:
  print("create job success, job_id is: " + job_id)
  
start_result=os.popen("/opt/newClient/Loader/loader-tools-1.99.3/sqoop-shell/sqoop2-shell -c 'start job -j "+job_id+" -s'").readlines()
if "FINE" in job_result[len(job_result)-1]:
  print("run job success")
else:
  print("run job faild")
  for i in start_result:
    print(i)
  os._exit(1)
  
print("spark-submit --master yarn pyhudi.py " + str(tableName) + " " + str(outputDirectory) +  " " + str(precombineFiled) + " " + str(recordkeyFiled) + " " + str(table_type) + " " + str(keygenerator) + " " + str(writeOperation) + " " + str(extractor) + " " + str(parallelism) + " " + str(writeMode) + " " + str(partitionFileds))

sync_result=os.popen("spark-submit --master yarn pyhudi.py " + str(tableName) + " " + str(outputDirectory) +  " " + str(precombineFiled) + " " + str(recordkeyFiled) + " " + str(table_type) + " " + str(keygenerator) + " " + str(writeOperation) + " " + str(extractor) + " " + str(parallelism) + " " + str(writeMode) + " " + str(partitionFileds)).readlines()

delete_result=os.popen("hdfs dfs -rm -r -f " + outputDirectory + "out_hive/").readlines()

print("sync finished")