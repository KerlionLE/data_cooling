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

def save_file(path: str, conf_info: list) -> None:
    """
    Сохранение файл
    :param path: путь к конфигу
    :param conf_info: конфиг с путём

    """

    os.makedirs(os.path.dirname(os.path.expandvars(path)), exist_ok=True)
    with open(os.path.expandvars(path), 'w') as f:
        f.write(json.dumps(conf_info))
