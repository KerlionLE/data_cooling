import os

import vertica_python
from data_cooling.krb import Kerberos
from datetime import datetime

# ------------------------------------------------------------------------------------------------------------------

def get_formated_file(path, **params):
    with open(os.path.expandvars(path)) as f:
        text = f.read()
    return text.format(**params)

def execute_sql(sql, conf_con_info):
    with vertica_python.connect(**conf_con_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

def get_last_date_cooling(conf_con_info, conf_query):
    sql = f'''
                    select schema_table_name, max(last_data_cooling)
                    from devdb.sandbox.data_cooling
                    where schema_table_name = {conf_query['schema_name']}.{conf_query['table_name']}
                    group by schema_table_name
                '''
    print(last_date_cooling)
    last_date_cooling = execute_sql(sql, conf_con_info)
    return last_date_cooling


def con_kerberus_vertica(conf_con_info, conf_krb_info, conf_query_info, sql_scripts_path):
    last_cooling_dates = {}
    current_date = datetime.now().date()
    current_date = current_date.strftime("%Y_%m_%d")

    with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
        for conf_query in conf_query_info:
            #if now_date - conf_query['last_date_cooling'] == conf_query['data_cooling_frequency']:
            last_date_cooling = get_last_date_cooling(conf_con_info, conf_query)
            print(last_date_cooling)
            if not conf_query['partition_expressions']:

                sql = get_formated_file(
                    sql_scripts_path['sql_export_without_partitions'],
                    schema_name=conf_query['schema_name'],
                    table_name=conf_query['table_name'],
                    filter_expression=conf_query['filter_expression'],
                    current_date=current_date
                )
            else:

                sql = get_formated_file(
                    sql_scripts_path['sql_export_with_partitions'],
                    schema_name=conf_query['schema_name'],
                    table_name=conf_query['table_name'],
                    filter_expression=conf_query['filter_expression'],
                    partition_expressions=conf_query['partition_expressions'],
                    current_date=current_date
                )
            execute_sql(sql, conf_con_info)
            last_cooling_dates[f"{conf_query['schema_name']}.{conf_query['table_name']}"] = current_date
            #else:
            #pass
    return last_cooling_dates
