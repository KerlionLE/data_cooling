import os
import json

from krbticket import KrbCommand, KrbConfig

from data_cooling.config_manager import AVAILABLE_FORMAT_MANAGER
from data_cooling.connect_manager import AVAILABLE_DB_CONNECTIONS, DBConnection


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


def get_formated_file(path: str, **params) -> str:
    """
    Open sql команд из папки sql
    :param path: путь к файлу
    :param **params: конфигурации, которые вставляеются в файл

    :return: Файл со вставленными параметрами
    """

    with open(os.path.expandvars(path)) as f:
        text = f.read()
    return text.format(**params)


def save_file(path: str, conf_info: list) -> None:
    """
    Сохранение файл
    :param path: путь к конфигу
    :param conf_info: конфиг с путём

    """

    os.makedirs(os.path.dirname(os.path.expandvars(path)), exist_ok=True)
    with open(os.path.expandvars(path), 'w') as f:
        json.dump(conf_info, f) 


def get_config_manager(source_type: str, source_config: list) -> dict:
    """
    get_config_manager - для проверки и запуска ConfigManager
    :param source_type: тип конфига
    :param source_config: источник

    :return: ConfigManager - объект
    """
    config_manager_cls = AVAILABLE_FORMAT_MANAGER.get(source_type)

    if config_manager_cls is None:
        raise ValueError(
            f'''UNKNOWN FORMAT: {source_type}. AVAILABLE TYPES:{','.join(AVAILABLE_FORMAT_MANAGER)}''',
        )
    return config_manager_cls(config=source_config)


def get_connect_manager(con_type: str, con_config: list) -> DBConnection:
    """
    get_connect_manager - для проверки и запуска DBConnection:

    :param con_type: тип con
    :param con_config: источник

    :return: DBConnection - объект
    """
    db_connection_cls = AVAILABLE_DB_CONNECTIONS.get(con_type)

    if db_connection_cls is None:
        raise ValueError(
            f'''UNKNOWN DB_TYPE: {con_type}. AVAILABLE TYPES:{','.join(AVAILABLE_FORMAT_MANAGER)}''',
        )
    return db_connection_cls(**con_config)
