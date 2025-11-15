from pyhive import hive
from contextlib import contextmanager
import time
import random
import sys
import traceback
import logging

hosts = ["xxx.xxx.xxx.xxx", "xxx.xxx.xxx.xxx"]
hiveserverPort = 21066
currentUser = 'hive'
currentDb = 'default'
authMode = None
serviceName = None
domain = None

maxRetryTimes = 3
retryInterval = 1

retryNodes = []
def getConn():
    allNodes = hosts.copy()
    times = 0
    while True:
        length = len(allNodes)
        index = random.randint(0, length - 1)
        try:
            connection = hive.Connection(host=allNodes[index], port=hiveserverPort, username=currentUser, database=currentDb, auth=authMode, kerberos_service_name=serviceName, krbhost=domain)
            return connection
        except (Exception) as e:
            retryNodes.append(allNodes[index])
            if length == 1:
                #already tried all nodes, next reset allNodes and connect to a random node
                allNodes = hosts.copy()
            else:
                #next will try to connect the other nodes
                del allNodes[index]

            times += 1
            if times < maxRetryTimes:
                time.sleep(retryInterval)
            else:
                for tryNode in retryNodes:
                    print("Could not connect to any of [('" + tryNode + "', " + str(hiveserverPort) + ")]")
                traceback.print_exc()
                return None

#disable logging during retry, only all nodes fail will print error message
@contextmanager
def disable_logging():
    logging.disable(logging.CRITICAL)
    try:
        yield
    finally:
        logging.disable(logging.NOTSET)

with disable_logging():
    connection = getConn()
if connection is None:
    sys.exit(1)

cursor = connection.cursor()
cursor.execute('show tables')
for result in cursor.fetchall():
    print(result)

