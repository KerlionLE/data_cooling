import vertica_python
from data_cooling.krb import Kerberos

def get_sql(path, conf_query):
    with open(path) as f:
        sql = f.read()
        sql = sql.format(schema_name=conf_query['schema_name'],
                         table_name=conf_query['table_name'],
                         filter_expression=conf_query['filter_expression'])
    return(sql)

def execute_sql(sql,conf_con_info):
    with vertica_python.connect(**conf_con_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

def con_kerberus_vertica(conf_con_info, conf_krb_info, conf_query_info):

    for conf_query in conf_query_info:
        with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
                    if conf_query_info['partition_expressions'] is None:

                        sql = get_sql('airflow/libs/sqls/export_without_partitions.sql', conf_query)
                        execute_sql(sql, conf_con_info)

                    else:
                        sql = get_sql('airflow/libs/sqls/export_with_partitions.sql', conf_query)
                        execute_sql(sql, conf_con_info)
