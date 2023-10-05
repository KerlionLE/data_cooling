import vertica_python
from data_cooling.krb import Kerberos

krb_config = {
                'principal':'a001cd-etl-vrt-hdp@DEV002.LOCAL',
                'keytab':'/usr/local/airflow/data/data_cooling/vrt_hdp.keytab'
             }

def con_kerberus_vertica(conn_info):
    with Kerberos(krb_config['principal'], krb_config['keytab']):
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute(f'''EXPORT TO PARQUET(directory='webhdfs:///data/vertica/ODS_LEAD_GEN_TST/KW_WORD_ENTITY_FRAME_TST2', compression='snappy') AS SELECT * FROM ODS_LEAD_GEN.KW_WORD_ENTITY_FRAME;''')
                data = cur.fetchall()
                print(data)