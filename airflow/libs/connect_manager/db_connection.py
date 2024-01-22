class DBConnection:
    """DBConnection для разных типов con"""

    def __init__(self, **config):
        """
        Инициализация con
        :param **config: конфиг с подключением

        """
        pass

    def apply_script_vrt(self, script: str) -> str:
        """
        apply_script - запуск скипта
        :param script:  скрипт

        """
        raise NotImplementedError
    
    def apply_script_hdfs(self, script: str, conf_krb_info: list) -> str:
        """
        apply_script - запуск скипта
        :param script:  скрипт
        :param  conf_krb_info: информация о конфиге соединения с Керберосом

        """
        raise NotImplementedError
    
    def apply_script_airflow_hdfs(self) -> str:
        """
        apply_script_airflow_hdfs - запуск скипта

        """
        raise NotImplementedError
