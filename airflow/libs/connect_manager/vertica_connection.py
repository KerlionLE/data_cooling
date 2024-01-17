import vertica_python

from .db_connection import DBConnection
from krbticket import KrbCommand, KrbConfig

class Kerberos:
    def __init__(self, principal, keytab, **kwargs):
        self.principal = principal
        self.keytab = keytab

    def kinit(self):
        kconfig = KrbConfig(principal=self.principal, keytab=self.keytab)
        KrbCommand.kinit(kconfig)

    def destroy(self):
        kconfig = KrbConfig(principal=self.principal, keytab=self.keytab)
        KrbCommand.kdestroy(kconfig)

    def __enter__(self):
        self.kinit()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.destroy()


class VerticaConnection(DBConnection):
    """Класс VerticaConnection для con к Вертике"""

    def __init__(self, **config):
        """
        init VerticaConnection
        :param **config: конфиг con

        """
        super(VerticaConnection, self).__init__(**config)
        self.__conn_info = config

    def apply_script_vrt(self, script: str) -> list:
        """
        apply_script_vrt создание курсора - запусе скрипта - autocommit
        Реализация: a = cur.fetchall() используется с select, а cur.fetchall() используется - поймать ошибку в dml(несколько скриптов) или ещё один select в файле
        :param script: sql запрос

        :return: результат sql запроса
        """
        with vertica_python.connect(**self.__conn_info) as conn:
            result = []
            with conn.cursor() as cur:

                cur.execute(script)
                result.append(cur.fetchall())
                while cur.nextset():
                    result.append(cur.fetchall())

            if not conn.autocommit:
                conn.commit()

            return result
    
    def apply_script_hdfs(self, script: str, conf_krb_info: list) -> list:
        """
        apply_script - запуск скипта.  Реализация: a = cur.fetchall() используется с select, а cur.fetchall() используется - поймать ошибку в dml(несколько скриптов) или ещё один select в файле
        :param script: скрипт - sql запрос из вертики в hdfs 
        :param conf_krb_info: конфиг подключения к через кебрерос

        """

        with Kerberos(conf_krb_info['principal'], conf_krb_info['keytab']):
            print('Привет')
            import kerberos
            print('!!!!!!!!!!', kerberos.__file__)
            with vertica_python.connect(**self.__conn_info) as conn:
                result = []
                with conn.cursor() as cur:

                    cur.execute(script)
                    result.append(cur.fetchall())
                    while cur.nextset():
                        result.append(cur.fetchall())

                if not conn.autocommit:
                    conn.commit()

                return result
