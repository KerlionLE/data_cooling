import logging

from .conf_manager import ConfigManager

from pydg.core.session import Session
from pydg.data_catalog.repo import Repo
from pydg.data_catalog.model.dicts import DataCatalogEntityType, EntityStatus


class PhysicalObjectCoolParams:
    def __init__(self,
                 id,
                 uuid,
                 physicalObjectId,
                 coolingType,
                 shouldDeleteSourceData,
                 coolingDepthDays,
                 coolingFrequency,
                 coolingFilterTimeColumnId,
                 coolingFilterTimeColumnName,
                 coolingFilterExpression,
                 coolingPartitionExpression,
                 coolingIsActive
                 ):
        self.id = id
        self.uuid = uuid
        self.physicalObjectId = physicalObjectId
        self.coolingType = coolingType
        self.shouldDeleteSourceData = shouldDeleteSourceData
        self.coolingDepthDays = coolingDepthDays
        self.coolingFrequency = coolingFrequency
        self.coolingFilterTimeColumnId = coolingFilterTimeColumnId
        self.coolingFilterTimeColumnName = coolingFilterTimeColumnName
        self.coolingFilterExpression = coolingFilterExpression
        self.coolingPartitionExpression = coolingPartitionExpression
        self.coolingIsActive = coolingIsActive

class PhysicalObjectCoolResult:
    def __init__(self,
                 id,
                 uuid,
                 physicalObjectCoolParamsId,
                 coolingLastDate,
                 coolingHdfsTarget
                 ):
        self.id = id
        self.uuid = uuid
        self.physicalObjectCoolParamsId = physicalObjectCoolParamsId
        self.coolingLastDate = coolingLastDate
        self.coolingHdfsTarget = coolingHdfsTarget

def cooling_type_to_dict(obj):
    return 'FULLCOPY'

def cool_params_to_dict(obj):
    d = {}
    for name, value in obj.__dict__.items():
        d[name] = value if name != 'coolingType' else cooling_type_to_dict(value)
    return d

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

        data_list_cool_parms = []
        for d in get_cool_parms['items']:
            data_list_cool_parms.append(cool_params_to_dict(d))
        
        print(data_list_cool_parms)
        id_objs_cool_parms = [d.get('obj_id') for d in data_list_cool_parms]
        print(id_objs_cool_parms)

        get_cool_results = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
            payload=request_cool_results
        )
        data_list_cool_results = []
        for d in get_cool_results['items']:
            data_list_cool_results.append(cool_params_to_dict(d))
        
        print(data_list_cool_results)

        return get_cool_parms, get_cool_results
