import os
from datetime import datetime, timedelta

from airflow import DAG

from operators.krb_livy_operator import KrbLivyOperator

DAG_NAME = os.path.splitext(os.path.basename(__file__))[0]

DEFAULT_ARGS = {
    'owner': 'romanovskiimv',
    'start_date': datetime(2024, 2, 4),
    'retries': 0,
    'task_concurrency': 5,
    'pool': 'tech_pool',
    'queue': 'afk8s_tech_queue',
}

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


with DAG(**DAG_CONFIG) as dag:
    livy_python_task = KrbLivyOperator(
        task_id='TRIGGER_PYSPARK_JOB',
        file='hdfs://s001cd-hdp-n01.dev002.local:8020/user/a001cd-ldgnpsp/lead_gen/scripts/inverted_index_ciq.py',
        jars=[
            'hdfs://s001cd-hdp-n01.dev002.local:8020/user/a001cd-ldgnpsp/spark-vertica-connector-assembly-3.3.5.jar',
        ],
        py_files=[
            'hdfs://s001cd-hdp-n01.dev002.local:8020/user/a001cd-ldgnpsp/lead_gen/scripts/data_proc.py',
            'hdfs://s001cd-hdp-n01.dev002.local:8020/user/a001cd-ldgnpsp/lead_gen/scripts/keywords.py',
            'hdfs://s001cd-hdp-n01.dev002.local:8020/user/a001cd-ldgnpsp/lead_gen/scripts/spark_init.py',
            'hdfs://s001cd-hdp-n01.dev002.local:8020/user/a001cd-ldgnpsp/lead_gen/scripts/nifi_request.py',
        ],
        driver_memory='6g',
        driver_cores=2,
        executor_memory='6g',
        executor_cores=2,
        num_executors=2,
        queue='users',
        conf={
            'spark.yarn.nodemanager.vmem-check-enabled': False,
            'spark.executor.memoryOverhead': 1638,
            'spark.rpc.message.maxSize': 2047,
            'spark.driver.maxResultSize': '2g',
            'spark.executor.extraJavaOptions': '-XX:+UseG1GC',
            'spark.driver.extraJavaOptions': '-XX:+UseG1GC',
            'spark.yarn.appMasterEnv.PYSPARK3_PYTHON': '/home/a001cd-ldgnpsp/venvs/default/bin/python3',
            'spark.yarn.appMasterEnv.PYSPARK3_DRIVER_PYTHON': '/home/a001cd-ldgnpsp/venvs/default/bin/python3',
            'spark.sql.shuffle.partitions': 192,
            'spark.default.parallelism': 192,
            'spark.pyspark.virtualenv.bin.path': '/home/a001cd-ldgnpsp/venvs/default/bin/',
            'spark.pyspark.python': '/home/a001cd-ldgnpsp/venvs/default/bin/python3',
            'spark.pyspark.driver.python': '/home/a001cd-ldgnpsp/venvs/default/bin/python3',
            'spark.submit.deployMode': 'cluster',
        },
        livy_conn_id='spark3_livy',
        polling_interval=30,
        keytab_path='a001cd-etl-vrt-hdp@DEV002.LOCAL',
        principal='/usr/local/airflow/data/data_cooling/vrt_hdp.keytab',
        extra_options={
            'verify': '/usr/local/airflow/certs/DevRootCA.pem',
        },
    )
