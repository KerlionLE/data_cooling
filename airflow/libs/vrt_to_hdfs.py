import vertica_python
from data_cooling.krb import Kerberos

krb_config = {
                'principal':'a001cd-etl-vrt-hdp@DEV002.LOCAL',
                'keytab':'/home/a001cd-ldgnpsp/tmp/hdp_vrt.keytab'
             }

def con_kerberus_vertica(**conn_info):
    with Kerberos(krb_config['principal'], krb_config['keytab']):
        with vertica_python.connect(**conn_info) as conn:
            with conn.cursor() as cur:
                cur.execute('select 1;')
                data = cur.fetchall()
                print(data)
