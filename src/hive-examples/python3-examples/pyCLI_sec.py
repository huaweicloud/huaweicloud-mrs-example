from pyhive import hive
connection = hive.Connection(host='hiveserverIp', port=hiveserverPort, username='hive', database='default', auth='KERBEROS', kerberos_service_name="hive", krbhost='hadoop.hadoop.com')
cursor = connection.cursor()
cursor.execute('show tables')
for result in cursor.fetchall():
    print(result)