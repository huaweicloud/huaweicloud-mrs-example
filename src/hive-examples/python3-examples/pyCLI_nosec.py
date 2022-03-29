from pyhive import hive
connection = hive.Connection(host='hiveserverIp', port=hiveserverPort, username='hive', database='default', auth=None, kerberos_service_name=None, krbhost=None)
cursor = connection.cursor()
cursor.execute('show tables')
for result in cursor.fetchall():
    print(result)