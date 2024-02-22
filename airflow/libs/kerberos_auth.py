from krbticket import KrbCommand, KrbConfig


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
