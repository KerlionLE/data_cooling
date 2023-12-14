import logging
import pytz

from datetime import datetime
from croniter import croniter

from .config_manager import AVAILABLE_FORMAT_MANAGER
from .connect_manager import AVAILABLE_DB_CONNECTIONS, DBConnection
from .utils import get_formated_file

# ------------------------------------------------------------------------------------------------------------------

def get_config_manager(source_type: str,
                       source_config: list) -> dict:
    """
    get_config_manager - ConfigManager
    :param source_type: тип конфига
    :param source_config: источник

    :return: ConfigManager - объект
    """
    config_manager_cls = AVAILABLE_FORMAT_MANAGER.get(source_type)

    if config_manager_cls is None:
        raise Exception(
            f'''UNKNOWN FORMAT: {source_type}. AVAILABLE TYPES: {','.join([c_name for c_name in AVAILABLE_FORMAT_MANAGER])}''',
        )
    return config_manager_cls(config=source_config)

def get_connect_manager(con_type: str,
                        con_config: list) -> DBConnection:
    """
    Соединение с источником (реализован) через класс
    :param con_type: тип con
    :param con_config: источник

    :return: DBConnection - объект
    """
    db_connection_cls = AVAILABLE_DB_CONNECTIONS.get(con_type)

    if db_connection_cls is None:
        raise Exception(
            f'''UNKNOWN DB_TYPE: {con_type}. AVAILABLE TYPES: {','.join([c_name for c_name in AVAILABLE_DB_CONNECTIONS])}''',
        )
    return db_connection_cls(**con_config)

# ------------------------------------------------------------------------------------------------------------------


def filter_objects(config: dict, system_tz: str) -> list:
    """
    Фильтрует словарь относительно частоты загрузки данных, сравнивая его с config timezone - airflow в utc, вертика в utc +3
    :param config: конфиг репликации
    :param system_tz: таймзона в конфиге

    :return: возвращает лист - отфильтрованный конфиг с учётом частоты
    """
    filtered_objects = []
    for conf in config:

        last_date_cooling = conf['last_date_cooling']
        update_freq = conf['data_cooling_frequency']

        if update_freq is None:
            logging.error(
                f''' Репликация таблицы - {conf['schema_name']}.{conf['table_name']}, не будет выполнена, так как в config нет указание шедулера ''',
            )
            continue

        if not last_date_cooling:
            conf['is_new'] = True
            conf['last_tech_load_ts'] = None
            filtered_objects.append(conf)
            continue

        conf['is_new'] = False
        last_tech_load_ts = datetime.strptime(last_date_cooling, '%Y-%m-%d %H:%M:%S')
        now = datetime.now(pytz.timezone(system_tz)).replace(tzinfo=None)
        update_freq = croniter(update_freq, last_tech_load_ts).get_next(datetime)

        if now >= update_freq:
            filtered_objects.append(conf)
        else:
            logging.info(
                f'''Время по частоте ещё не пришло для таблицы - {conf['schema_name']}.{conf['table_name']} ''',
            )

    return filtered_objects

# ------------------------------------------------------------------------------------------------------------------

def get_max_load_ts(filtered_objects: list,
                         db_connection_src: DBConnection,
                         sql_scripts_path_select: str) -> list:
    """
    Select из основных таблиц выборки. Забираем max(tech_load_ts)
    :param filtered_objects: лист - отфильтрованный конфиг с учётом частоты
    :param db_connection_src: объект соединения
    :param sql_scripts_path_select: путь к sql скрипту - select

    :return: возвращает лист - с максимальной датой(tech_load_ts) в схема-таблица
    """
    filtered_objects_with_maxdate = []

    for conf in filtered_objects:
        col_name = conf.get('tech_ts_column_name') or 'tech_load_ts'
        sql_select = get_formated_file(
            sql_scripts_path_select,
            column_name=col_name,
            schema_name=conf['schema_name'],
            table_name=conf['table_name'],
        )
        try:
            max_date = db_connection_src.apply_sql(sql_select)[0]
        except Exception as e:
            logging.error(
                f'''Таблица {conf['schema_name']}.{conf['table_name']} не существует или столца tech_ts нет - {e}''',
            )
            continue

        if max_date and max_date[0][0] is not None:
            conf['actual_max_tech_load_ts'] = max_date[0][0].strftime('%Y-%m-%d %H:%M:%S')
            filtered_objects_with_maxdate.append(conf)

        elif max_date[0][0] is None: 
            logging.warning(
                f'''У пользователя нет доступа к таблице - {conf['schema_name']}.{conf['table_name']} ''',
            )

        else:
            logging.warning(
                f'''Таблица {conf['schema_name']}.{conf['table_name']} пустая ''',
            )
    return filtered_objects_with_maxdate

# ------------------------------------------------------------------------------------------------------------------

def preprocess_config_checks_con_dml(conf: list, db_connection_config_src: DBConnection) -> None:
    """
    :param con_type: тип con к базе
    :param schema_name_regestry: название схемы технической с историей
    :param table_name_regestry: название таблицы технической с историей
    :param auxiliary_sql_paths: пути всех sql файлов
    :param db_connection_config_src: config src con
    :param db_connection_config_tgt: config trg con
    :param system_tz: таймзаона в конфиге

    """

    copy_to_vertica = conf['auxiliary_sql_paths']['sql_copy_to_vertica']
    create_external_table_hdfs = conf['auxiliary_sql_paths']['sql_create_external_table_hdfs']
    delete_without_partitions = conf['auxiliary_sql_paths']['sql_delete_without_partitions']
    sql_delete_with_partitions = conf['auxiliary_sql_paths']['sql_delete_with_partitions']
    export_with_partitions = conf['auxiliary_sql_paths']['sql_export_with_partitions']
    export_without_partitions = conf['auxiliary_sql_paths']['sql_export_without_partitions']
    get_max_tech_load_ts = conf['auxiliary_sql_paths']['sql_get_max_tech_load_ts']

    con_type = conf['source_system']['system_type']
    source_type = conf['replication_objects_source']['source_type']
    source_config = conf['replication_objects_source']['source_config']
    system_tz = conf['source_system']['system_config']['system_tz']

    'Step 1 - создание conn, берем конфиг, выписываем список таблиц'
    db_connection_src = get_connect_manager(con_type, db_connection_config_src)

    'Step 2 - берём конфиг'
    config_manager = get_config_manager(source_type, source_config)
    config = config_manager.get_config()
    logging.info(config)

    'Step 3 - фильтруем по частоте'
    filter_object = filter_objects(config, system_tz)
    logging.info(filter_object)

    'Step 4 - текущая макс дата в проде'
    max_tech_load_ts = get_max_load_ts(filter_object, db_connection_src, get_max_tech_load_ts)
    logging.info(max_tech_load_ts)

    print(max_tech_load_ts)

