"""
Загрузка данных (Охлаждение) Vertica --> HDFS
v1.0.0 by romanovskiimv
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_cooling.vrt_to_hdfs import con_kerberus_vertica

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
    'default_args': DEFAULT_ARGS,
    'doc_md': __doc__,
}

with DAG(**DAG_CONFIG) as dag:
    vertica_to_hdfs = PythonOperator(
        task_id=f'CON_KERBERUS_VERTICA',
        trigger_rule='none_skipped',
        python_callable=con_kerberus_vertica,
        op_kwargs=
        {
            {
                'conf_con_info': f'{{{{ var.json.{DAG_NAME}.conf_con_info }}}}',
                'conf_krb_info': f'{{{{ var.json.{DAG_NAME}.conf_krb_info }}}}',
                'conf_query_info': f'{{{{ var.json.{DAG_NAME}.conf_query_info }}}}'
            }
        }

    )

    # Засунуть данные через sql
    # надо добавить параметры в конфиг: - в зависимости от этих параметров реализовать запуск или пропуск охлаждения конкретной таблицы
    # последняя дата охлаждения данных
    # периодичность запуска охлаждения

    vertica_to_hdfs
