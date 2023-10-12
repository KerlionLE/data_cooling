import os

import vertica_python
from data_cooling.krb import Kerberos
from datetime import datetime

from airflow.models import Variable, DagModel, Connection

# ------------------------------------------------------------------------------------------------------------------

def set_airflow_variable(name: str, value: str):
    Variable.set(name, value)

def get_formated_file(path, **params):
    with open(os.path.expandvars(path)) as f:
        text = f.read()
    return text.format(**params)

def execute_sql(sql, conf_con_info):
    with vertica_python.connect(**conf_con_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)

def con_kerberus_vertica(conf_con_info, conf_krb_info, conf_query_info, sql_scripts_path):
    last_cooling_dates = {}
    current_date = datetime.now().date()

    with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
        for conf_query in conf_query_info:
            #if now_date - conf_query['last_date_cooling'] == conf_query['data_cooling_frequency']:
            if conf_query['partition_expressions'] is None:

                sql = get_formated_file(
                    sql_scripts_path['sql_export_without_partitions'],
                    schema_name=conf_query['schema_name'],
                    table_name=conf_query['table_name'],
                    filter_expression=conf_query['filter_expression']
                )
            else:

                sql = get_formated_file(
                    sql_scripts_path['sql_export_with_partitions'],
                    schema_name=conf_query['schema_name'],
                    table_name=conf_query['table_name'],
                    filter_expression=conf_query['filter_expression'],
                    partition_expressions=conf_query['partition_expressions']
                )
            execute_sql(sql, conf_con_info)
            last_cooling_dates[f"{conf_query['schema_name']}.{conf_query['table_name']}"] = current_date
        else:
            pass

    return last_cooling_dates
