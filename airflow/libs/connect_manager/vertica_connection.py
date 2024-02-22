import vertica_python
from krbticket import KrbCommand, KrbConfig

from .db_connection import DBConnection


class KerberosAuth:
    """Класс KerberosAuth для con к Вертике и hdfs"""

    def __init__(self, principal: str, keytab_path: str) -> None:
        """
        Инициализация класса
        :param principal: необходимый параметр для подключения через керберос
        :param keytab_path: необходимый параметр для подключения через керберос

        """

        self.principal = principal
        self.keytab_path = keytab_path
        self._krb_config = None

    @property
    def krb_config(self) -> KrbConfig:
        """
        krb_config

        :return: возвращает krb конфиг
        """
        if self._krb_config is None:
            self._krb_config = KrbConfig(principal=self.principal, keytab=self.keytab_path)

        return self._krb_config

    def kinit(self) -> None:
        """kinit"""
        KrbCommand.kinit(self.krb_config)

    def kdestroy(self) -> None:
        """kdestroy"""
        KrbCommand.kdestroy(self.krb_config)

    def __enter__(self) -> None:
        """enter"""
        self.kinit()

    def __exit__(self, exc_type: str, exc_val: str, exc_tb: str) -> None:
        """
        exit

        :param exc_type: запуск типа
        :param exc_val: запусе значения
        :param exc_tb: запуск таблицы
        """
        self.kdestroy()


class VerticaConnection(DBConnection):
    """Класс VerticaConnection для con к Вертике"""

    def __init__(self, **config):
        """
        init VerticaConnection
        :param **config: конфиг con

        """
        super(VerticaConnection, self).__init__(**config)
        self.__conn_info = config

    def apply_script_hdfs(self, script: str, conf_krb_info: list) -> list:
        """
        apply_script - запуск скипта.  Реализация: a = cur.fetchall() используется с select, а cur.fetchall() используется - поймать ошибку в dml(несколько скриптов) или ещё один select в файле
        :param script: скрипт - sql запрос из вертики в hdfs
        :param conf_krb_info: конфиг подключения к через кебрерос

        :return: результат sql запроса
        """

        with KerberosAuth(conf_krb_info['principal'], conf_krb_info['keytab']), vertica_python.connect(**self.__conn_info) as conn:
            result = []
            with conn.cursor() as cur:

                cur.execute(script)
                result.append(cur.fetchall())
                while cur.nextset():
                    result.append(cur.fetchall())

            if not conn.autocommit:
                conn.commit()

            return result
