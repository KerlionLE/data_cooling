import os
import json

from .config_manager import AVAILABLE_FORMAT_MANAGER
from .connect_manager import AVAILABLE_DB_CONNECTIONS, DBConnection


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
        f.write(json.dumps(conf_info))


def get_config_manager(source_type: str, source_config: list) -> dict:
    """
    get_config_manager - для проверки и запуска ConfigManager
    :param source_type: тип конфига
    :param source_config: источник

    :return: ConfigManager - объект
    """
    config_manager_cls = AVAILABLE_FORMAT_MANAGER.get(source_type)

    if config_manager_cls is None:
        raise Exception(
            f'''UNKNOWN FORMAT: {source_type}. AVAILABLE TYPES: {','.join((c_name for c_name in AVAILABLE_FORMAT_MANAGER))}''',
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
        raise Exception(
            f'''UNKNOWN DB_TYPE: {con_type}. AVAILABLE TYPES: {','.join((c_name for c_name in AVAILABLE_DB_CONNECTIONS))}''',
        )
    return db_connection_cls(**con_config)
