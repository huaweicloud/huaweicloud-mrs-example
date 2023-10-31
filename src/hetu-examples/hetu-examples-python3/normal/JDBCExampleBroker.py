import jaydebeapi

driver = "io.trino.jdbc.TrinoDriver"

# need to change the value based on the cluster information
url = "jdbc:trino://192.168.37.61:29861,192.168.37.62:29861/hive/default?serviceDiscoveryMode=hsbroker"
user = "YourUserName"
tenant = "YourTenant"
jdbc_location = "Your file path of the jdbc jar"

sql = "show catalogs"

if __name__ == '__main__':
    conn = jaydebeapi.connect(driver, url, {"user": user,
                                            "SSL": "false",
                                            "tenant": tenant},
                              [jdbc_location])
    curs = conn.cursor()
    curs.execute(sql)
    result = curs.fetchall()
    print(result)
    curs.close()
    conn.close()