import logging
import sys
from datetime import datetime

try:
    from pydg.core.session import Session
    from pydg.data_catalog.repo import Repo
    from pydg.data_catalog.model.dicts import DataCatalogEntityType
except ImportError:
    logging.warning("Импорт: нет библиотеки 'pydg'")

from .conf_manager import ConfigManager


def conn_to(config: list) -> str:
    """
    Подключение к дата каталогу
    :param config: креды для подключения

    :return: конект
    """

    base_url = config['base_url']  # URL прода, теста или дева
    root_ca_path = config['root_ca_path']
    username = config['username']
    password = config['password']

    logger = logging.getLogger('data_catalog')
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(sh)
    logger.info('Start')

    cur_session = Session(logger)  # create and start API session
    if not cur_session.start(baseUrl=base_url, username=username, password=password, rootCA=root_ca_path):
        logger.error('Failed to start session')
        raise

    logger.handlers.clear()
    return Repo(cur_session, logger)


def type_to_dict(obj: str) -> str:
    """
    Функция реализована для работы со структурой объекта(один из объектов охлаждения/разогрева) 
    В объекте(список данных, которые мы получаем из дата_каталога).
    Идея заключается в том что - каталог возвращает один из объектов класса как класс
    Cледовательно нужно его достать и обрабоать
    :param obj: объект для изменения

    :return: возвращает правильное значение
    """
    return obj.__dict__.values


def params_to_dict(obj: str) -> dict:
    """
    Функция превращения класса в словарь
    Кака это работает - берём и итерируемся по всем классам и преобразуем в дикт
    :param obj: Класс объекта

    :return: словарь
    """

    d = {}
    for name, value in obj.__dict__.items():
        d[name] = value if name not in ['coolingType', 'heatingType'] else value.value
    return d


def compound_coolparams_coolresult(repo: str) -> list:
    """
    Обработка конфига охлаждения - json формата из data catalog
    :param repo: сессия con

    :return: лист с обектами для охлаждения
    """

    request_cool_parms = {
        'query': {
            'coolingIsActive': True,
        },
        'page': 1,
        'pageSize': 300,
    }

    request_cool_results = {
        'page': 1,
        'pageSize': 300,
    }

    # 1 Работа с обектом PhysicalObjectCoolParams
    get_cool_parms = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectCoolParams.value,
        payload=request_cool_parms,
    )
    print(get_cool_parms)
    logging.info(get_cool_parms)

    data_list_cool_parms = []
    for d in get_cool_parms['items']:
        data_list_cool_parms.append(params_to_dict(d))

    # 1.1.1 Берём список таблиц для того не тащить имена таб.
    id_objs_cool_parms = [d.get('physicalObjectId')
                          for d in data_list_cool_parms]

    # 1.1 Работа с обектом PhysicalObjectCoolResult
    get_cool_results = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
        payload=request_cool_results,
    )
    print(get_cool_results)
    logging.info(get_cool_results)

    data_list_cool_results = []
    for d in get_cool_results['items']:
        data_list_cool_results.append(params_to_dict(d))

    # 1.2 Объединение PhysicalObjectCoolParams и PhysicalObjectCoolResult
    data_list_cool = []
    for a in data_list_cool_parms:
        for b in data_list_cool_results:
            if a['id'] == b['physicalObjectCoolParamsId']:
                a['coolingLastDate'] = b['coolingLastDate']
                a['coolingHdfsTarget'] = b['coolingHdfsTarget']
                a['PhysicalObjectCoolResultId'] = b['id']
                data_list_cool.append(a)

        if a.get('coolingLastDate', False) is False:
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
        'page': 1,
        'pageSize': 300,
    }

    # 2 Работа с обектом PhysicalObjectHeatParams
    get_heat_param = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectHeatParams.value,
        payload=request_heating_parms,
    )
    print(get_heat_param)
    logging.info(get_heat_param)

    data_list_heat_parms = []
    for d in get_heat_param['items']:
        data_list_heat_parms.append(params_to_dict(d))

    # 2.1 Работа с обектом PhysicalObjectHeatResult
    get_heat_results = repo.readEntity(
        entityType=DataCatalogEntityType.PhysicalObjectHeatResult.value,
        payload=request_heating_results,
    )
    print(get_heat_results)
    logging.info(get_heat_results)

    data_list_heat_results = []
    for d in get_heat_results['items']:
        data_list_heat_results.append(params_to_dict(d))

    # 2.2 Объединение PhysicalObjectHeatParams и PhysicalObjectHeatResult
    data_list_heat = []
    for a in data_list_heat_parms:
        for b in data_list_heat_results:
            if a['id'] == b['physicalObjectHeatParamsId']:
                a['heatingExternalTableName'] = b['heatingExternalTableName']
                a['isAlreadyHeating'] = b['isAlreadyHeating']
                a['PhysicalObjectHeatResultId'] = b['id']
                data_list_heat.append(a)

        if a.get('heatingExternalTableName', False) is False:
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

    for a in data_list_cool:
        for b in data_list_heat:
            if a['physicalObjectId'] == b['physicalObjectId']:
                a['physicalObjectCoolParamsId'] = a.get('id')
                a['physicalObjectHeatParamsId'] = b.get('id')
                a['PhysicalObjectHeatResultId'] = b.get('PhysicalObjectHeatResultId') or None
                a['PhysicalObjectCoolResultId'] = a.get('PhysicalObjectCoolResultId') or None
                a['heatingType'] = b.get('heatingType')
                a['heatingDepthDays'] = b.get('heatingDepthDays')
                a['heatingStartDate'] = b.get('heatingStartDate')
                a['heatingEndDate'] = b.get('heatingEndDate')
                a['heatingIsActive'] = b.get('heatingIsActive')
                a['heatingExternalTableName'] = b.get('heatingExternalTableName') or None
                a['isAlreadyHeating'] = b.get('isAlreadyHeating') or None
                data_list.append(a)

        if a.get('heatingType', False) is False:
            a['physicalObjectCoolParamsId'] = a.get('id')
            data_list.append(a)

    return data_list


def get_physicalobject(id_objs_cool_parms: list, repo: str) -> list:
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


def get_physicalgroup(id_objs_objects: list, repo: str) -> list:
    """
    Обработка конфига разогрева - json формата из data catalog берем 2 конфига объед.
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

        :return: лист
        """
        repo = conn_to(self.config)

        # 3 Объединение coolresult и heatresult
        data_list_cool, id_objs_cool_parms = compound_coolparams_coolresult(repo)
        data_list_heat = compound_heatparams_heatresult(repo)

        # 4 Объединение Heat и Cool
        data_list = compound_heat_cool(data_list_cool, data_list_heat)

        # 5 Работа с обектом PhysicalObject
        data_list_oblects, id_objs_objects = get_physicalobject(id_objs_cool_parms, repo)

        # 6 Работа с обектом PhysicalGroup
        data_list_group = get_physicalgroup(id_objs_objects, repo)

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

        data_final = []

        for a in data_list_all:
            conf_final = {}
            conf_final['physicalObjectCoolParamsId'] = a.get('physicalObjectCoolParamsId')
            conf_final['physicalObjectHeatParamsId'] = a.get('physicalObjectHeatParamsId')
            conf_final['PhysicalObjectHeatResultId'] = a.get('PhysicalObjectHeatResultId')
            conf_final['PhysicalObjectCoolResultId'] = a.get('PhysicalObjectCoolResultId')
            conf_final['schema_name'] = a.get('physicalNameGroup')
            conf_final['table_name'] = a.get('physicalName')
            conf_final['cooling_type'] = a.get('coolingType')
            conf_final['replication_policy'] = a.get('shouldDeleteSourceData')
            conf_final['cooling_depth'] = a.get('coolingDepthDays')
            conf_final['last_date_cooling'] = str(a.get('coolingLastDate')) or None
            conf_final['data_cooling_frequency'] = a.get('coolingFrequency')
            conf_final['tech_ts_column_name'] = a.get('coolingFilterTimeColumnName')
            conf_final['filter_expression'] = a.get('coolingFilterExpression')
            conf_final['partition_expressions'] = a.get('coolingPartitionExpression')
            conf_final['heating_type'] = a.get('heatingType') or None
            conf_final['heating_depth'] = a.get('heatingDepthDays') or None
            conf_final['heating_date_start'] = str(a.get('heatingStartDate')) or None
            conf_final['heating_date_end'] = str(a.get('heatingEndDate')) or None
            conf_final['hdfs_path'] = a.get('heatingExternalTableName')
            conf_final['isAlreadyHeating'] = a.get('isAlreadyHeating') or 0
            data_final.append(conf_final)

        return data_final

    def put_data_cooling(self, conf: list) -> None:

        """
        Заолняем таблицы result для охлаждения
        :param conf: возможные параметры конфига

        """
        repo = conn_to(self.config)
        data_type = '%Y-%m-%d %H:%M:%S'

        if not conf['PhysicalObjectCoolResultId'] or conf['PhysicalObjectCoolResultId'] is None or conf['PhysicalObjectCoolResultId'] == 'None':
            post_result = repo.createEntity(
                                        entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
                                        entityDraft={
                                            "physicalObjectCoolParamsId": conf['physicalObjectCoolParamsId'],
                                            "coolingLastDate": datetime.strptime(conf['date_end_cooling_depth'], data_type),
                                            "coolingHdfsTarget": conf['hdfs_path'],
                                        },
                                        )
            logging.info(post_result)
        else:
            res = repo.updateEntity(entityType=DataCatalogEntityType.PhysicalObjectCoolResult.value,
                                    entityDraft={
                                                            "id":  conf['PhysicalObjectCoolResultId'],
                                                            "coolingLastDate": datetime.strptime(conf['date_end_cooling_depth'], data_type),
                                                })
            logging.info(res)

    def put_data_heating(self, conf: list = None) -> None:

        """
        Заолняем таблицы result для охлаждения
        :param conf: возможные параметры конфига

        """
        repo = conn_to(self.config)

        if not conf['PhysicalObjectHeatResultId'] or conf['PhysicalObjectHeatResultId'] is None or conf['PhysicalObjectHeatResultId'] == 'None':
            post_result = repo.createEntity(entityType=DataCatalogEntityType.PhysicalObjectHeatResult.value,
                                            entityDraft={
                                                        "physicalObjectHeatParamsId": conf['physicalObjectHeatParamsId'],
                                                        "heatingExternalTableName": f'''{conf['schema_name']}.{conf['table_name']}''',
                                                        "isAlreadyHeating": True,
                                            })
            logging.info(post_result)
        else:
            res = repo.updateEntity(entityType=DataCatalogEntityType.PhysicalObjectHeatResult.value,
                                    entityDraft={
                                                    "id": conf['PhysicalObjectHeatResultId'],
                                                    "isAlreadyHeating": True,
                                                })
            logging.info(res)
