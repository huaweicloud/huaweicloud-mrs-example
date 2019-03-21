import sys
import os
import logging

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

# Add path for local "gen-py/hbase" for the pre-generated module
gen_py_path = os.path.abspath('gen-py')
sys.path.append(gen_py_path)

from hbase import THBaseService
from hbase.ttypes import *

print("Thrift2 Demo")
print("Please check \"README.txt\" before Running the sample code.")
print("This demo assumes you have a table called \"example\" with a column family called \"family1\" and \"family2\" ")

host = "IP"
port = 9090
framed = False
saslServiceName = "thrift"
secureMode = True
table = "example"
krb5cc_file = "/tmp/krb5cc_0"
userPrincipal = "hbaseuser@0387E634_69F0_406F_8633_33E788C1E162.COM"
keytableFilePath = "/opt/client/HBase/python-example/user.keytab"


# login
def login():
    if os.path.exists(krb5cc_file):
        os.remove(krb5cc_file)
    with krbContext(using_keytab=True,
                    principal=userPrincipal,
                    keytab_file=keytableFilePath,
                    ccache_file=krb5cc_file):
        pass


# create a connection
def createConnection():
    socket = TSocket.TSocket(host, port)

    if framed:
        transport = TTransport.TFramedTransport(socket)
    else:
        if secureMode:
            transport = TTransport.TSaslClientTransport(socket, host, saslServiceName)
        else:
            transport = TTransport.TBufferedTransport(socket)

    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = THBaseService.Client(protocol)
    transport.open()
    return client, transport


# Single put
def singlePut():
    put = TPut(row="row2009", columnValues=[TColumnValue(family="family1", qualifier="qualifier1", value="value1")])
    print("Putting:", put)
    client.put(table, put)
    put = TPut(row="row2009", columnValues=[TColumnValue(family="family2", qualifier="bb", value="1value2")])
    print("Putting:", put)
    client.put(table, put)


# get data
def getData():
    get = TGet(row="row2009")
    print("Getting:", get)
    result = client.get(table, get)
    for columnValue in result.columnValues:
        print("result for get:", result.row, columnValue.family, columnValue.qualifier, columnValue.value)
        print("")


# Batch put
def batchPut():
    putList = []
    for num in range(0, 5):
        put = TPut(row="row" + str(num),
                   columnValues=[TColumnValue(family="family1", qualifier="aa", value=str(num) + "value1")])
        putList.append(put)
        put = TPut(row="row" + str(num),
                   columnValues=[TColumnValue(family="family2", qualifier="bb", value=str(num) + "value2")])
        putList.append(put)

    print("putlist: ", putList)
    client.putMultiple(table, putList)

    print("")


# scan data
def scanData():
    scan = TScan(startRow="row", stopRow="row2", columns=[TColumn(family='family1', qualifier='aa')])
    scannerId = client.openScanner(table, scan)
    print("scan with startRow=\"row\", stopRow=\"row2\", and special column(family1:aa): ", scannerId)
    results = client.getScannerRows(scannerId, 100000)
    for result in results:
        for columnValue in result.columnValues:
            print("result for scan:", result.row, columnValue.family, columnValue.qualifier, columnValue.value)
    client.closeScanner(scannerId)

    print("")


# scan data with single filter
def scanDataWithSingleFilter():
    scan = TScan(startRow="row1", stopRow="row999",
                 filterString="SingleColumnValueFilter('family2','bb',=,'binary:1value2')")
    scannerId = client.openScanner(table, scan)
    print("scan with filter: family2:bb=1value2", scannerId)
    results = client.getScannerRows(scannerId, 100000)
    for result in results:
        # print("result: ",result)
        for columnValue in result.columnValues:
            print("result for scan:", result.row, columnValue.family, columnValue.qualifier, columnValue.value)
    client.closeScanner(scannerId)
    print("")


# scan data with composite filter
def scanDataWithCompositeFilter():
    scan = TScan(startRow="row1", stopRow="row999",
                 filterString="SingleColumnValueFilter('family2','bb',>,'binary:1value2') AND SingleColumnValueFilter('family2','bb',<,'binary:3value2')")
    scannerId = client.openScanner(table, scan)
    print("scanner with filter family2:bb between 1value2 and 3value2")
    results = client.getScannerRows(scannerId, 100000)
    for result in results:
        print("result for scan:", result.row, result.columnValues[0].family, result.columnValues[0].qualifier,
              result.columnValues[0].value)
        print("result for scan:", result.row, result.columnValues[1].family, result.columnValues[1].qualifier,
              result.columnValues[1].value)
    client.closeScanner(scannerId)


# close connection
def closeConnection():
    transport.close()


if secureMode:
    import kerberos
    from krbcontext.context import krbContext
    # create connection
    try:
        client, transport = createConnection()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        client, transport = createConnection()

    # Single put
    try:
        singlePut()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        singlePut()

    # get data
    try:
        getData()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        getData()

    # Batch put
    try:
        batchPut()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        batchPut()

    # scan data
    try:
        scanData()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        scanData()

    # scan data with single filter
    try:
        scanDataWithSingleFilter()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        scanDataWithSingleFilter()

    # scan data with composite filter
    try:
        scanDataWithCompositeFilter()
    except kerberos.GSSError as e:
        logging.exception(e)
        print("relogin...")
        login()
        scanDataWithCompositeFilter()
    # close connection
    closeConnection()
else:
    client, transport = createConnection()
    singlePut()
    getData()
    batchPut()
    scanData()
    scanDataWithSingleFilter()
    scanDataWithCompositeFilter()
    closeConnection()
