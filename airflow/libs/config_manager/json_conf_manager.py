from .conf_manager import ConfigManager


class JsonConfManager(ConfigManager):
    """Класс Обработки конфига - включает в себя get и save"""

    def get_config(self, path: str = None) -> dict:
        """
        Обработка конфига - json формата
        :param path: путь к конфигу

        :return: лист внутри json
        """

        return self.config
