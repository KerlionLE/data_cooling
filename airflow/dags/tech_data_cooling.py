import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from dwh_utils.airflow.common import get_dag_name

from operators.python_virtualenv_artifactory_operator import PythonVirtualenvCurlOperator

from data_cooling.vrt_hdfs_cooling import preprocess_config_checks_con_dml

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
    'schedule_interval': '* * 1 * *',
    'concurrency': 5,
    'max_active_runs': 1,
    'catchup': False,
    'render_template_as_native_obj': True,
    'default_args': DEFAULT_ARGS,
    'doc_md': __doc__,
}


def get_replication_config(dag_name: str, env_name: str, replication_names: str) -> dict:
    """
    Функция реализована для получения из Variable Airflow конфига
    :param dag_name: название дага
    :param env_name: название среды
    :param replication_names: название конфига в Variable Airflow

    :return: возвращает конфиг
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
    Функция реализована для получения конфига(con) с помощью BaseHook
    :param dag_name: название дага
    :param env_name: название среды
    :param replication_names: название конфига
    :param system_type: тип системы

    :return: возвращает конфиг(con) - ввиде('host' - 'port' - 'database' - 'user' - 'password')
    """
    replication_config = get_replication_config(
        dag_name, env_name, replication_names)

    try:
        con = BaseHook.get_connection(
            replication_config[system_type]['system_config']['connection_config']['connection_conf']['conn_id'])
        return {
            'host': con.host,
            'port': con.port,
            'database': con.schema,
            'user': con.login,
            'password': con.password,
        }
    except Exception as e:
        logging.warning(f"The conn_id `con_id` isn't defined: {e}")


def get_qty_worker(dag_name: str, env_name: str, replication_names: str) -> dict:
    """
    Функция реализована для получения из кофига кол. воркеров для airflow (для того чтобы запускать паралельно несколько конфига охлаждения)
    :param dag_name: название дага
    :param env_name: название среды
    :param replication_names: название конфига

    :return: Возвращает колличесво воркеров airflow на которых будет рабоать ran_dml
    """

    replication_config = get_replication_config(
        dag_name, env_name, replication_names)
    return replication_config['workers']


PYPI_REQUIREMENTS = [
    'pydantic>=2.0.0',
]

with DAG(**DAG_CONFIG) as dag:

    ukd_requirements = [
        {'lib_name': 'pydg', 'version': 'v0.3.19', 'storage': 'non-standard'},
        {'lib_name': 'dg_utils', 'version': '1.0.3', 'storage': 'standard'},
    ]

    inegration_name = 'data_cooling'

    preprocess_config_checks_con_dml = PythonVirtualenvCurlOperator(
        task_id='preprocess_config_checks_con_dml',
        pypi_requirements=PYPI_REQUIREMENTS,
        ukd_requirements=ukd_requirements,
        connection_params={
                'login': r'{{ conn.artifactory_pypi_rc.login }}',
                'password': r'{{ conn.artifactory_pypi_rc.password }}',
                'host': r'{{ conn.artifactory_pypi_rc.host }}',
        },
        pip_config={
            'index-url': f'{{{{ var.json.dg.pip_config.{AIRFLOW_ENV}.index_url }}}}',
            'trusted-host': f'{{{{ var.json.dg.pip_config.{AIRFLOW_ENV}.trusted_host }}}}',
            'extra-index-url': f'{{{{ var.json.dg.pip_config.{AIRFLOW_ENV}.extra_index_url }}}}',
            'extra-url-password': r'{{ conn.artifactory_pypi_rc.password }}',
            'extra-url-login': r'{{ conn.artifactory_pypi_rc.login }}',
        },
        python_callable=preprocess_config_checks_con_dml,
        op_kwargs={
            'conf': f'{{{{ var.json.{DAG_NAME}.{AIRFLOW_ENV}.{inegration_name} }}}}',
            'db_connection_config_src': get_conn(dag_name=DAG_NAME, env_name=AIRFLOW_ENV, replication_names=inegration_name, system_type='source_system'),
        },
    )

    preprocess_config_checks_con_dml
