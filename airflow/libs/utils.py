import os
import json


def get_formated_file(path: str, **params) -> str:
    """
    Open sql команд из папки sql
    :param path: путь к файлу
    :param **params: конфигурации, которые вставляеются в файл

    :return: list corresponding Vertica column names
    """

    with open(os.path.expandvars(path)) as f:
        text = f.read()
    return text.format(**params)


def open_file(path: str) -> dict:
    """
    Открытие файла
    :param path: путь к файлу

    :return: dict
    """

    file_path_exp = os.path.expandvars(path)
    with open(file_path_exp, 'r') as f:
        return json.load(f)


def save_file(path: str, conf_info: list) -> None:
    """
    Сохранение файл
    :param path: путь к конфигу
    :param conf_info: конфиг с путём

    """

    file_path = os.path.expandvars(path)
    dir_path = os.path.dirname(file_path)
    os.makedirs(dir_path, exist_ok=True)
    with open(file_path, 'w') as f:
        f.write(json.dumps(conf_info))
