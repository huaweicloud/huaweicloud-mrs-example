
from pyhs2.connections import Connection
import time
import logging

logging.basicConfig(filename = '/var/log/python-client.log',
                    level = logging.WARN, filemode = 'a',
                    format = '%(asctime)s - %(levelname)s - (%(pathname)s,%(lineno)s)  - %(message)s')  

class HAConnection():
    #connection = None
    
    def __init__(self, hosts = None, port = 10000, authMechanism = None, user = None,
                 password = None, database = None, configuration = None, timeout = 10000):
        if hosts is None:
            raise ValueError('hosts is None .')

        hostlist = hosts
        if isinstance(hosts, list):
            hosts = tuple(hosts)
        elif isinstance(hosts, tuple):
            pass
        else:
            raise ValueError('hosts is not a tuple or a list .')
        
        isConnectted = False
        for i in range(0, 3*len(hostlist)):
            host = select_random_hiveserver(hostlist)
            try:
                self.connection = Connection(
                    host, port, authMechanism, user, password,
                    database, configuration, timeout)
                isConnectted = True
                print("Connect to hive server successful! host:%s,"
                      " port:%s" % (host, port))
                logging.debug("Connect to hive server successful!")
                break
            except Exception, e:
                print("Failed to connect host : %s ,"
                      "error message is : %s" % (host, e))
                logging.warning("Failed to connect host : %s ,"
                                "error message is : %s" % (host, e))
            if isConnectted:
                break
            else:
                time.sleep(1)
        
        if not isConnectted:
            logging.error("Failed to connect hive server.")
            raise HAConnectionError("Failed to connect hive server.")
        
    def getConnection(self):
        return self.connection
        
    def close(self):
        if self.connection is not None:
            self.connection.close()
            
    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        if self.connection is not None:
            self.connection.close()
        
class HAConnectionError(Exception):
    def __init__(self, value):
        self.value = value
      
    def __str__(self):  
        return repr(self.value)

def select_random_hiveserver(clients):
	import random
	clientsNum = len(clients)
	if (clientsNum > 0):
		index = random.randint(0, clientsNum - 1)
		client = clients[index]
	else:
		client = clients[0]
	return client
    
