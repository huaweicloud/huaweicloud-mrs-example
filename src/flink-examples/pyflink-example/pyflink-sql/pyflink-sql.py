import logging
import sys
import os
from pyflink.table import (EnvironmentSettings, TableEnvironment)


def read_sql(file_path):
    if not os.path.isfile(file_path):
        raise TypeError(file_path + " does not exist")

    all_the_text = open(file_path).read()
    return all_the_text


def exec_sql():
    # 提交之前修改sql路径。
    file_path = "datagen2kafka.sql"
    sql = read_sql(file_path)
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    statement_set = t_env.create_statement_set()
    sqlArr = sql.split(";")
    for sqlStr in sqlArr:
        sqlStr = sqlStr.strip()
        if sqlStr.lower().startswith("create"):
            print("---------create---------------")
            print(sqlStr)
            t_env.execute_sql(sqlStr)
        if sqlStr.lower().startswith("insert"):
            print("---------insert---------------")
            print(sqlStr)
            statement_set.add_insert_sql(sqlStr)

    statement_set.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    exec_sql()
