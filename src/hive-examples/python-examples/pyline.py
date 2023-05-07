#!/bin/bash
from pyhs2.haconnection import HAConnection
import sys

#"xxx.xxx.xxx.xxx"is IP address
if len(sys.argv)<3:
	print "Need hive server address and sql..."
	sys.exit()
host_ip = (sys.argv[1]).split(':')
hosts = [host_ip[0], host_ip[0]]
port = int(host_ip[1])
sql = sys.argv[2]
conf = {"krb_host":"hadoop.hadoop.com", "krb_service":"hive"}
try:
    with HAConnection(hosts = hosts,
                       port = port,
                       authMechanism = "KERBEROS",
                       configuration = conf,timeout = 60000) as haConn:
        with haConn.getConnection() as conn:
            with conn.cursor() as cur:
                # Show databases
                #print cur.getDatabases()
                
                # Execute query
                cur.execute(sql)
                
                # Fetch table results
                for i in cur.fetch():
                    print i
                    
except Exception, e:
    print e
    sys.exit(1)
