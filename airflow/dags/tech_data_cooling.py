import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from data_cooling.vrt_hdfs_cooling import preprocess_config_checks_con_dml
from dwh_utils.airflow.common import get_dag_name

AIRFLOW_ENV = os.environ['AIRFLOW_ENV']

DEFAULT_ARGS = {
    'owner': 'romanovskiimv',
    'start_date': datetime(2023, 10, 17),
    'retries': 0,
    'task_concurrency': 5,
    'pool': 'tech_pool',
    'queue': 'afk8s_tech_queue',
}

DAG_NAME = get_dag_name(__file__)

DAG_CONFIG = {
    'dag_id': DAG_NAME,
    'schedule_interval': '0 0 * * *',
    'concurrency': 5,
    'max_active_runs': 1,
    'catchup': False,
    'render_template_as_native_obj': True,
    'default_args': DEFAULT_ARGS,
    'doc_md': __doc__,
}

def get_replication_config(dag_name: str, env_name: str, replication_names: str) -> dict:
    """
    Из Variable Airflow забираем конфиг
    :param dag_name: название дага
    :param env_name: название среды
    :param replication_names: конфиг

    :return: возвращает лист - отфильтрованный конфиг с учётом частоты
    """
    variables = Variable.get(dag_name, deserialize_json=True)

    env_variables = variables.get(env_name)
    if env_variables is None:
        raise Exception(
            f"CAN NOT FIND ENVIROMENT '{env_name}' IN variables.json")

    replication_config = env_variables.get(replication_names)
    if replication_config is None:
        raise Exception(
            f"CAN NOT FIND REPLICATION '{replication_names}' IN variables.json")

    return replication_config

def get_conn(dag_name: str, env_name: str, replication_names: str, system_type: str) -> dict:
    """
    Из get_replication_config забираем конфиг con
    :param dag_name: название дага
    :param env_name: название среды
    :param replication_names: конфиг
    :param system_type: название среды

    :return: конфиг
    """
    replication_config = get_replication_config(
        dag_name, env_name, replication_names)
    
    try:
        con = BaseHook.get_connection(replication_config[system_type]['system_config']['connection_config']['connection_conf']['conn_id'])
        return print({
            'host': con.host,
            'port': con.port,
            'database': con.schema,
            'user': con.login,
            'password': con.password,
        })
    except Exception as e:
        logging.warning(f"The conn_id `con_id` isn't defined: {e}")

def get_qty_worker(dag_name: str, env_name: str, replication_names: str) -> dict:
    """
    Забираем из кофига кол. воркеров для airflow
    :param dag_name: название дага
    :param env_name: название среды
    :param replication_names: конфиг

    :return: возвращает лист - количесво воркеров, которое будем использлвать
    """

    replication_config = get_replication_config(
        dag_name, env_name, replication_names)
    return replication_config['workers']


with DAG(**DAG_CONFIG) as dag:

    inegration_name = 'data_cooling'

    preprocess_config_checks_con_dml = PythonOperator(
        task_id='preprocess_config_checks_con_dml',
        trigger_rule='none_skipped',
        python_callable=preprocess_config_checks_con_dml,
        op_kwargs={
            'conf': f'{{{{ var.json.{DAG_NAME}.{AIRFLOW_ENV}.{inegration_name} }}}}',
            'db_connection_config_src': get_conn(dag_name=DAG_NAME, env_name=AIRFLOW_ENV, replication_names=inegration_name, system_type='source_system'),
        },
    )