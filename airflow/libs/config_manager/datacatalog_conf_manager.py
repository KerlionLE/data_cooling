import logging

from pydg.core.session import Session
from pydg.data_catalog.repo import Repo
from pydg.data_catalog.model.dicts import DataCatalogEntityType

from .conf_manager import ConfigManager


def type_to_dict(obj: str) -> str:
    """
    Функция реализована для работы со структурой обект в объекте
    :param obj: объект для изменения

    :return: возвращает правильное значение
    """

    return 'FULLCOPY'


def params_to_dict(obj: str) -> dict:
    """
    Функция превращения класса в словарь
    :param obj: Класс объекта

    :return: словарь
    """

    d = {}
    for name, value in obj.__dict__.items():
        d[name] = value if name != 'coolingType' or name != 'heatingType' else type_to_dict(
            value)
    return d


def compound_coolparams_coolresult(repo: str) -> list:
    """
    Обработка конфига охлаждения - json формата из data catalog
    :param repo: сессия con

    :return: лист с обектами для охлаждения
    """

    request_cool_parms = {
        'quer': {
            'coolingIsActive': True,
        },
        'page': 1,
        'pageSize': 300,
    }

    request_cool_results = {
        'query': {
            'physicalObjectCoolParamsId': 39,
        },
        'page': 1,
        'pageSize': 300,
    }

    # 1 Работа с обектом PhysicalObjectCoolParams
    get_cool_parms = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectCoolParams.value,
        payload=request_cool_parms,
    )

    data_list_cool_parms = []
    for d in get_cool_parms['items']:
        data_list_cool_parms.append(params_to_dict(d))

    # 1.1.1 Берём список таблиц для того не тащить имена таблиц
    id_objs_cool_parms = [d.get('physicalObjectId')
                          for d in data_list_cool_parms]

    # 1.1 Работа с обектом PhysicalObjectCoolResult
    get_cool_results = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
        payload=request_cool_results,
    )
    data_list_cool_results = []
    for d in get_cool_results['items']:
        data_list_cool_results.append(params_to_dict(d))

    # 1.2 Объединение PhysicalObjectCoolParams и PhysicalObjectCoolResult
    data_list_cool = []
    for a in data_list_cool_parms:
        if len(data_list_cool_results) != 0:
            for b in data_list_cool_results:
                if a['id'] == b['physicalObjectCoolParamsId']:
                    a['coolingLastDate'] = b['coolingLastDate']
                    a['coolingHdfsTarget'] = b['coolingHdfsTarget']
                    data_list_cool.append(a)
        else:
            data_list_cool.append(a)

    return data_list_cool, id_objs_cool_parms


def compound_heatparams_heatresult(repo: str) -> list:
    """
    Обработка конфига разогрева - json формата из data catalog берем 2 конфига объединяем
    :param repo: сессия con

    :return: лист
    """

    request_heating_parms = {
        'query': {
            'heatingIsActive': True,
        },
        'page': 1,
        'pageSize': 300,
    }

    request_heating_results = {
        'query': {
            'physicalObjectHeatParamsId': 39,
        },
        'page': 1,
        'pageSize': 300,
    }

    # 2 Работа с обектом PhysicalObjectHeatParams
    get_heat_param = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectHeatParams.value,
        payload=request_heating_parms,
    )

    data_list_heat_parms = []
    for d in get_heat_param['items']:
        data_list_heat_parms.append(params_to_dict(d))

    # 2.1 Работа с обектом PhysicalObjectHeatResult
    get_heat_results = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectHeatResult.value,
        payload=request_heating_results,
    )

    data_list_heat_results = []
    for d in get_heat_results['items']:
        data_list_heat_results.append(params_to_dict(d))

    # 2.2 Объединение PhysicalObjectHeatParams и PhysicalObjectHeatResult
    data_list_heat = []
    for a in data_list_heat_parms:
        if len(data_list_heat_results) != 0:
            for b in data_list_heat_results:
                if a['id'] == b['physicalObjectHeatParamsId']:
                    a['heatingExternalTableName'] = b['heatingExternalTableName']
                    a['isAlreadyHeating'] = b['isAlreadyHeating']
                    data_list_heat.append(a)
        else:
            data_list_heat.append(a)

    return data_list_heat

def compound_heat_cool(data_list_cool: list, data_list_heat: list) -> list:
    """
    Обработка конфига разогрева - охлаждени и разогрева
    :param data_list_cool: конфиг охлаждения
    :param data_list_heat: конфиг разогрева

    :return: лист
    """
    data_list = []
    temporary_heating = 'temporary_heating'
    for a in data_list_cool:
        for b in data_list_heat:
            if a['physicalObjectId'] == b['physicalObjectId']:
                a[{temporary_heating}]['heatingType'] = b['heatingType']
                a[{temporary_heating}]['heatingDepthDays'] = b['heatingDepthDays']
                a[{temporary_heating}]['heatingStartDate'] = b['heatingStartDate']
                a[{temporary_heating}]['heatingEndDate'] = b['heatingEndDate']
                a[{temporary_heating}]['heatingIsActive'] = b['heatingIsActive']
                a[{temporary_heating}]['heatingExternalTableName'] = b['heatingExternalTableName']
                a[{temporary_heating}]['isAlreadyHeating'] = b['isAlreadyHeating']
                data_list.append(a)
            else:
                data_list.append(a)

def physicalobject(id_objs_cool_parms: list, repo: str) -> list:
    """
    Обработка конфига разогрева - json формата из data catalog берем 2 конфига объединяем
    :param id_objs_cool_parms: список таблиц для охлаждения
    :param repo: сессия con

    :return: лист
    """

    request_objects = {
            'query': {
                'id': id_objs_cool_parms,
            },
            'page': 1,
            'pageSize': 300,
        }

    get_objects = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObject.value,
        payload=request_objects,
    )

    data_list_oblects = []
    for d in get_objects['items']:
        data_list_oblects.append(params_to_dict(d))

    # 4.1.1 Берём список таблиц для того не тащить имена схем
    id_objs_objects = [d.get('group') for d in data_list_oblects]

    return data_list_oblects, id_objs_objects

def physicalgroup(id_objs_objects: list, repo: str) -> list:
    """
    Обработка конфига разогрева - json формата из data catalog берем 2 конфига объединяем
    :param id_objs_objects: список таблиц для охлаждения
    :param repo: сессия con

    :return: лист
    """

    request_group = {
            'query': {
                'id': id_objs_objects,
            },
            'page': 1,
            'pageSize': 300,
        }

    get_group = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalGroup.value,
        payload=request_group,
    )

    data_list_group = []
    for d in get_group['items']:
        data_list_group.append(params_to_dict(d))

    return data_list_group

class DataCatalogConfManager(ConfigManager):
    """Класс Обработки конфига - включает в себя get и save"""

    def get_config(self, conf: list = None) -> dict:
        """
        Обработка конфига - json формата из data catalog берем 2 конфига объединяем
        :param conf: возможные параметры конфига

        """
        base_url = self.config['base_url']  # URL прода, теста или дева
        root_ca_path = self.config['root_ca_path']
        username = self.config['username']
        password = self.config['password']

        logger = logging.getLogger('data_catalog')
        logger.setLevel(logging.DEBUG)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        logger.addHandler(sh)
        logger.info('Start')

        session = Session(logger)  # create and start API session
        if not session.start(baseUrl=base_url, username=username, password=password, rootCA=root_ca_path):
            logger.error('Failed to start session')
            return

        repo = Repo(session, logger)
        logger.info('Execute query')

        # 3 Объединение coolresult и heatresult
        data_list_cool, id_objs_cool_parms = compound_coolparams_coolresult(repo)
        data_list_heat = compound_heatparams_heatresult(repo)

        # 4 Объединение Heat и Cool
        data_list = compound_heat_cool(data_list_cool, data_list_heat)

        # 5 Работа с обектом PhysicalObject
        data_list_oblects, id_objs_objects = physicalobject(id_objs_cool_parms, repo)

        # 6 Работа с обектом PhysicalGroup
        data_list_group = physicalgroup(id_objs_objects, repo)

        # 7 Объединение объектов обектов PhysicalGroup и PhysicalObject
        data_list_oblects_group = []
        for a in data_list_oblects:
            for b in data_list_group:
                if a['group'] == b['id']:
                    a['physicalNameGroup'] = b['physicalName']
                    data_list_oblects_group.append(a)

        # 6 Объединение Всего
        data_list_all = []
        for a in data_list:
            for b in data_list_oblects_group:
                if a['physicalObjectId'] == b['id']:
                    a['physicalName'] = b['physicalName']
                    a['physicalNameGroup'] = b['physicalNameGroup']
                    data_list_all.append(a)
