import logging

from .conf_manager import ConfigManager

from pydg.core.session import Session
from pydg.data_catalog.repo import Repo
from pydg.data_catalog.model.dicts import DataCatalogEntityType


def type_to_dict(obj):
    return 'FULLCOPY'

def params_to_dict(obj):
    d = {}
    for name, value in obj.__dict__.items():
        d[name] = value if name != 'coolingType' or name != 'heatingType' else type_to_dict(value)
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

        request_heating_parms = {
            "query": {
                "heatingIsActive": True
            },
            "page": 1,
            "pageSize": 300
        }

        request_heating_results = {
            "query": {
                "physicalObjectHeatParamsId": 39
            },
            "page": 1,
            "pageSize": 300
        }

        #1 Работа с обектом PhysicalObjectCoolParams
        get_cool_parms = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectCoolParams.value,
            payload=request_cool_parms
        )

        data_list_cool_parms = []
        for d in get_cool_parms['items']:
            data_list_cool_parms.append(params_to_dict(d))
        
        id_objs_cool_parms = [d.get('physicalObjectId') for d in data_list_cool_parms]

        #1.1 Работа с обектом PhysicalObjectCoolResult
        get_cool_results = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
            payload=request_cool_results
        )
        data_list_cool_results = []
        for d in get_cool_results['items']:
            data_list_cool_results.append(params_to_dict(d))

        #1.2 Объединение PhysicalObjectCoolParams и PhysicalObjectCoolResult
        data_list_cool = []
        for a in data_list_cool_parms: 
            for b in data_list_cool_results:
                if a['id'] == b['physicalObjectCoolParamsId']:
                   a['coolingLastDate'] = b['coolingLastDate']
                   a['coolingHdfsTarget'] = b['coolingHdfsTarget']
                   data_list_cool.append(a)

        print(data_list_cool)

        #2 Работа с обектом PhysicalObjectHeatParams
        get_heat_param = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectHeatParams.value,
            payload=request_heating_parms
        )

        data_list_heat_parms = []
        for d in get_heat_param['items']:
            data_list_heat_parms.append(params_to_dict(d))

        #2.1 Работа с обектом PhysicalObjectHeatResult
        get_heat_results = repo.readEntity(
            entityType=DataCatalogEntityType.PhysicalObjectHeatResult.value,
            payload=request_heating_results
        )

        data_list_heat_results = []
        for d in get_heat_results['items']:
            data_list_heat_results.append(params_to_dict(d))

        #2.2 Объединение PhysicalObjectHeatParams и PhysicalObjectHeatResult
        data_list_heat = []
        for a in data_list_heat_parms: 
            for b in get_heat_results:
                if a['id'] == b['physicalObjectCoolParamsId']:
                   a['coolingLastDate'] = b['coolingLastDate']
                   a['coolingHdfsTarget'] = b['coolingHdfsTarget']
                   data_list_heat.append(a)

        print(data_list_heat)

        #3 Объединение Heat и Cool
        data_list = []
        for a in data_list_cool: 
            for b in data_list_heat:
                if a['physicalObjectId'] == b['physicalObjectId']:
                   a['heatingType'] = b['heatingType']
                   a['heatingDepthDays'] = b['heatingDepthDays']
                   a['heatingStartDate'] = b['heatingStartDate']
                   a['heatingEndDate'] = b['heatingEndDate']
                   a['heatingIsActive'] = b['heatingIsActive']
                   data_list.append(a)

        print(data_list)

        return get_cool_parms, get_cool_results
