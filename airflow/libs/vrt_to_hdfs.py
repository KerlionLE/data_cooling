import ast

import vertica_python
from data_cooling.krb import Kerberos


def get_sql(**param):
    with open(...):
        ...


def execute_sql(**kwargs):
    ...


def con_kerberus_vertica(conf_con_info, conf_krb_info, conf_query_info):
    if conf_query_info['schema_name'] is None or conf_query_info['table_name'] is None:
        raise ValueError('Schema or Table name name must be defined')

    if conf_query_info['cooling_type'] is None or conf_query_info['depth'] is None:
        raise ValueError('Cooling type or depth must be defined')

    if conf_query_info['replication_policy'] != 0 or 1:
        raise ValueError('Replication policy must be 0 or 1')

    with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
        sql = get_sql(**{})
        execute_sql(sql)

        with vertica_python.connect(**conf_con_info) as conn:
            with conn.cursor() as cur:

                if conf_query_info['partition_expressions'] is None:
                    with open('airflow/libs/sqls/export_without_partitions.sql') as f:
                        sql = f.read()
                        formated_sql = sql.format(**{})
                    cur.execute(formated_sql)

                else:
                    cur.execute(
                        """
                            EXPORT TO PARQUET(directory='webhdfs:///data/vertica/%(schema_name)/%(table_name)', compression='snappy')
                            OVER(PARTITION BY part) 
                            AS
                            SELECT *, %(partition_expressions) as part
                            FROM %(schema_name).%(table_name)
                            WHERE 1=1 %(filter_expression);
                        """,
                        {
                            'partition_expressions': conf_query_info['partition_expressions'],
                            'filter_expression': conf_query_info['filter_expression'],
                            'schema_name': conf_query_info['schema_name'],
                            'table_name': conf_query_info['table_name']
                        }
                    )
