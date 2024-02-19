import logging

from .conf_manager import ConfigManager

from pydg.core.session import Session
from pydg.data_catalog.repo import Repo
from pydg.data_catalog.model.dicts import DataCatalogEntityType, EntityStatus


class DataCatalogConfManager(ConfigManager):
    """Класс Обработки конфига - включает в себя get и save"""

    def get_config(self, conf: list = None) -> dict:
        """
        Обработка конфига - json формата из data catalog
        :param path: путь к конфигу

        :return: лист внутри json
        """
        BASE_URL = 'https://dg.dev002.local/dc-blue'  # URL прода, теста или дева
        ROOT_CA_PATH = self.config["root_ca_path"]
        USERNAME = self.config["username"]
        PASSWORD = self.config["password"]

        logger = logging.getLogger('data_catalog')
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(sh)
        logger.info('Start')

        session = Session(logger)  # create and start API session
        if not session.start(baseUrl=BASE_URL, username=USERNAME, password=PASSWORD, rootCA=ROOT_CA_PATH):
            logger.error('Failed to start session')
            return

        repo = Repo(session, logger)
        logger.info('Execute query')

        request_cool_parms = {
            "query": {
                "coolingIsActive": True
            },
            "page": 1,
            "pageSize": 300
        }

        request_cool_results = {
            "query": {
                "physicalObjectCoolParamsId": 39
            },
            "page": 1,
            "pageSize": 300
        }

        get_cool_parms = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectCoolParams.value,
            payload=request_cool_parms
        )

        get_cool_results = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
            payload=request_cool_results
        )

        return get_cool_parms, get_cool_results
