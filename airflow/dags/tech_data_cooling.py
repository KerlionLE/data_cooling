"""
Загрузка данных (Охлаждение) Vertica --> HDFS
v1.0.0 by romanovskiimv
"""
import os
from datetime import datetime
import vertica_python

from data_cooling.krb import Kerberos
from airflow import DAG
from airflow.operators.python import PythonOperator

from data_cooling.vrt_to_hdfs import con_kerberus_vertica

# ------------------------------------------------------------------------------------------------------------------
def execute_sql(sql, conf_con_info, conf_krb_info):
    with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
        with vertica_python.connect(**conf_con_info) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

def update_last_cooling_dates(conf_con_info, xcom_value, conf_krb_info):

    #sql_script_1 = f''' 
    #                    CREATE TABLE IF NOT EXISTS devdb.sandbox.data_cooling
    #                        (
    #                            schema_table_name varchar(128),
    #                            last_data_cooling varchar(128)
    #                        );
    #                '''
    
    #execute_sql(sql_script_1, conf_con_info, conf_krb_info)
    print(xcom_value)
    sql_script_2 = "INSERT INTO devdb.sandbox.data_cooling (schema_table_name, last_data_cooling) VALUES "
    print(sql_script_2)
    values = []
    for key, value in xcom_value.items():
        values.append("('{}', '{}')".format(key, value))
        sql_script_2 += ", ".join(values)
    print(sql_script_2)
    sql_script_2 = "drop table sandbox.data_cooling;"
    execute_sql(sql_script_2, conf_con_info, conf_krb_info)
# ------------------------------------------------------------------------------------------------------------------

AIRFLOW_ENV = os.environ["AIRFLOW_ENV"]

DEFAULT_ARGS = {
    'owner': 'romanovskiimv',
    'start_date': datetime(2023, 10, 17),
    'retries': 0,
    'task_concurrency': 1,
    'pool': 'tech_pool'
}

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0].upper()

DAG_CONFIG = {
    'dag_id': DAG_NAME,
    'schedule_interval': '0 0 * * *',
    'concurrency': 1,
    'max_active_runs': 1,
    'catchup': False,
    'render_template_as_native_obj': True,
    'default_args': DEFAULT_ARGS,
    'doc_md': __doc__,
}

with DAG(**DAG_CONFIG) as dag:

    vertica_to_hdfs = PythonOperator(
        task_id=f'con_kerberus_vertica',
        trigger_rule='none_skipped',
        python_callable=con_kerberus_vertica,
        op_kwargs=
        {
            "conf_con_info": {
                "host": "{{ conn.vertica_staging.host }}",
                "port": "{{ conn.vertica_staging.port }}",
                "user": "a001cd-etl-vrt-hdp",
                "database": "{{ conn.vertica_staging.schema }}"
                },
            'conf_krb_info': f'{{{{ var.json.{DAG_NAME}.conf_krb_info }}}}',
            'sql_scripts_path': f'{{{{ var.json.{DAG_NAME}.sql_scripts_path }}}}',
            'conf_query_info': f'{{{{ var.json.{DAG_NAME}.conf_query_info }}}}'
        }

    )

    last_cooling_dates = PythonOperator(
        task_id=f'update_last_cooling_dates',
        trigger_rule='none_skipped',
        python_callable=update_last_cooling_dates,
        op_kwargs=
        {
            "conf_con_info": {
                "host": "{{ conn.vertica_staging.host }}",
                "port": "{{ conn.vertica_staging.port }}",
                "user": "a001cd-etl-vrt-hdp",
                "database": "{{ conn.vertica_staging.schema }}"
                },
            'conf_krb_info': f'{{{{ var.json.{DAG_NAME}.conf_krb_info }}}}',
            'xcom_value' : "{{ ti.xcom_pull(task_ids='con_kerberus_vertica') }}"
        }
    )

    vertica_to_hdfs >> last_cooling_dates
