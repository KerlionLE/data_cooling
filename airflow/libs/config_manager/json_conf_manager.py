from .conf_manager import ConfigManager


class JsonConfManager(ConfigManager):
    """Класс Обработки конфига - включает в себя get и save"""

    def get_config(self) -> dict:
        """
        Обработка конфига - json формата

        :return: лист внутри json
        """

        return self.config
