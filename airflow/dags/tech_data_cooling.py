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
            'conf_con_info':
                {
                "host": '{{ conn.vertica_staging.host }}',
                "port": '{{ conn.vertica_staging.port }}',
                "user": 'a001cd-etl-vrt-hdp',
                "database": '{{ conn.vertica_staging.schema }}'
                },
            'conf_krb_info':
                {
                "principal": 'a001cd-etl-vrt-hdp@DEV002.LOCAL',
                "keytab": '/usr/local/airflow/data/data_cooling/vrt_hdp.keytab'
                },
            'conf_query1_info':
                {
                "schema_name": 'ODS_LEADGEN_TST',
                "table_name": 'KW_WORD_ENTITY_FRAME_TST',
                "cooling_type": 'time_based',
                "replication_policy": 1,
                "depth": "24M",
                "filter_expression": 'AND tech_load_ts > {{ ts_date }}',
                "partition_expressions": 'substr(entity_index, 1, 1)'
                },
            'conf_query1_info':
                {
                "schema_name": 'ODS_LEADGEN',
                "table_name": 'KW_WORD_ENTITY_FRAME_TST',
                "cooling_type": 'time_based',
                "replication_policy": 1,
                "depth": "24M",
                "filter_expression": 'AND tech_load_ts > {{ ts_date }}',
                "partition_expressions": 'substr(entity_index, 1, 1)'
                }
            }
        )

        # Перенос в varaibales
        # Побить блоки пр схемам
        # Засунуть данные через sql

    vertica_to_hdfs
