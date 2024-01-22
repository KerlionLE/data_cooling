from .db_connection import DBConnection
import requests

class HdfsConnection(DBConnection):
    """Класс HdfsConnection для con к HDFS"""

    def __init__(self, **config):
        """
        init HdfsConnection
        :param **config: конфиг con

        """
        super(HdfsConnection, self).__init__(**config)
        self.__conn_info = config

    def apply_script_airflow_hdfs(self) -> list:
        """
        apply_script_airflow_hdfs запуск скрипта для переноса файлов
        :param PATH: sql запрос

        :return: результат sql запроса
        """
        url = self.__conn_info['HDFS_URL'] + self.__conn_info['HDFS_PATH'] + self.__conn_info['OPS']
        res = requests.get(url)
        print(res)
