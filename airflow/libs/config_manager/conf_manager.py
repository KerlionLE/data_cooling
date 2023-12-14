class ConfigManager:
    """ConfigManager для разных типов конфига"""

    def __init__(self, config: list) -> None:
        """
        Инициализация config
        :param config: config для реплицакии

        """
        self.config = config

    def get_config(self, **kwargstr) -> dict:
        """
        Обработка config
        :param **kwargstr: путь или конфиг с путём

        """
        raise NotImplementedError
