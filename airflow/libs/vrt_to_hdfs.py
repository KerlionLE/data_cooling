import vertica_python
from data_cooling.krb import Kerberos

def con_kerberus_vertica(CONF_CON_INFO, CONF_KRB_INFO, CONF_QUERY_INFO):

    if CONF_QUERY_INFO['schema_name'] is None or CONF_QUERY_INFO['table_name'] is None:
        raise ValueError('Schema or Table name name must be defined')

    if CONF_QUERY_INFO['cooling_type'] is None or CONF_QUERY_INFO['depth'] is None:
        raise ValueError('Cooling type or depth must be defined')

    if CONF_QUERY_INFO['replication_policy'] != 0 or 1 :
        raise ValueError('Replication policy must be 0 or 1')

    with Kerberos(CONF_KRB_INFO['principal'], CONF_KRB_INFO['keytab']):
        with vertica_python.connect(**CONF_CON_INFO) as conn:
            with conn.cursor() as cur:

                if CONF_QUERY_INFO['partition_expressions'] is None:
                    cur.execute(
                        """
                            EXPORT TO PARQUET(directory='webhdfs:///data/vertica/%(schema_name)/%(table_name)', compression='snappy') 
                            AS
                            SELECT * FROM %(schema_name).%(table_name)
                            WHERE 1=1 %(filter_expression);
                        """,
                                    {
                                        'filter_expression': CONF_QUERY_INFO['filter_expression'],
                                        'schema_name': CONF_QUERY_INFO['schema_name'],
                                        'table_name': CONF_QUERY_INFO['table_name']
                                    }
                                )

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
                                        'partition_expressions': CONF_QUERY_INFO['partition_expressions'],
                                        'filter_expression': CONF_QUERY_INFO['filter_expression'],
                                        'schema_name': CONF_QUERY_INFO['schema_name'],
                                        'table_name': CONF_QUERY_INFO['table_name']
                                    }
                                )
