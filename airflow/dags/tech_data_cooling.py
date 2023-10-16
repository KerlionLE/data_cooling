"""
Загрузка данных (Охлаждение) Vertica --> HDFS
v1.0.0 by romanovskiimv
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_cooling.vrt_to_hdfs import con_kerberus_vertica


def update_last_cooling_dates(last_cooling_dates, conf_query_info):
    updated = []
    for conf_query in conf_query_info:
        last_cooling_date = last_cooling_dates.get(f"{conf_query['schema_name']}.{conf_query['table_name']}")
        if last_cooling_date:
            conf_query['last_date_cooling'] = last_cooling_date

        updated.append(conf_query)

    # update airflow variable
    #Variable.set(name, value)

# ------------------------------------------------------------------------------------------------------------------

AIRFLOW_ENV = os.environ["AIRFLOW_ENV"]

DEFAULT_ARGS = {
    'owner': 'romanovskiimv',
    'start_date': datetime(2023, 10, 1),
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

    update_last_cooling_dates = PythonOperator(
        task_id=f'update_last_cooling_dates',
        trigger_rule='none_skipped',
        python_callable=update_last_cooling_dates,
        op_kwargs={
            '{{ ti.xcom_pull(task_ids=con_kerberus_vertica }}',
            f'{{{{ var.json.{DAG_NAME}.conf_query_info }}}}'
        }
    )

    vertica_to_hdfs >> update_last_cooling_dates
