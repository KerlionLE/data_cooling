import os

import vertica_python
from datetime import datetime
from krbticket import KrbCommand, KrbConfig

# ------------------------------------------------------------------------------------------------------------------

class Kerberos:
    def __init__(self, principal, keytab, **kwargs):
        self.principal = principal
        self.keytab = keytab

    def kinit(self):
        kconfig = KrbConfig(principal=self.principal, keytab=self.keytab)
        KrbCommand.kinit(kconfig)

    def destroy(self):
        kconfig = KrbConfig(principal=self.principal, keytab=self.keytab)
        KrbCommand.kdestroy(kconfig)

    def __enter__(self):
        self.kinit()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()
# ------------------------------------------------------------------------------------------------------------------

def get_formated_file(path, **params):
    with open(os.path.expandvars(path)) as f:
        text = f.read()
    return text.format(**params)

def execute_sql(sql, conf_con_info):
    with vertica_python.connect(**conf_con_info) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
# ------------------------------------------------------------------------------------------------------------------

def get_last_date_cooling(conf_query, sql_scripts_path):

    sql = get_formated_file(
        sql_scripts_path['get_last_date_cooling'],
        schema_name=conf_query['schema_name'],
        table_name=conf_query['table_name']
        )
    print(sql)
    return sql

def put_last_date_cooling(conf_con_info, conf_query, sql_scripts_path, new_last_date):

    sql = get_formated_file(
        sql_scripts_path['put_last_date_cooling'],
        schema_name=conf_query['schema_name'],
        table_name=conf_query['table_name']
        )
    values = []
    for key, value in new_last_date.items():
       values.append("('{}', '{}')".format(key, value))
       sql += ", ".join(values)
    execute_sql(sql, conf_con_info)
# ------------------------------------------------------------------------------------------------------------------

def con_kerberus_vertica(conf_con_info, conf_krb_info, conf_query_info, sql_scripts_path):
    last_cooling_dates = {}
    current_date = datetime.now().date()
    current_date = current_date.strftime("%Y-%m-%d")

    with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
        for conf_query in conf_query_info:
            sql = get_last_date_cooling(conf_query, sql_scripts_path)
            last_date_cooling = execute_sql(sql, conf_con_info)
            print(last_date_cooling)
            print(datetime.strptime(last_date_cooling.values(), '%d/%m/%y'))
            print(conf_query['data_cooling_frequency'])
            if (current_date - datetime.strptime(last_date_cooling.values(), '%d/%m/%y')).days == conf_query['data_cooling_frequency']:      
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
            else:
                print("Время ещё не прошло")
        if not last_cooling_dates:
            pass
        else:
            put_last_date_cooling(conf_con_info, conf_query, sql_scripts_path, last_cooling_dates)
    return last_cooling_dates
