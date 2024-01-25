import logging
import pytz

from datetime import datetime, timedelta
from croniter import croniter

from .connect_manager import DBConnection
from .utils import get_formated_file, get_connect_manager, get_config_manager
from .checkconf import chconf

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

        last_date_cooling = conf.get('last_date_cooling')
        update_freq = conf.get('data_cooling_frequency')

        if not last_date_cooling:
            conf['is_new'] = True
            conf['last_tech_load_ts'] = None
            filtered_objects.append(conf)
            continue

        conf['is_new'] = False
        last_tech_load_ts = datetime.strptime(
            last_date_cooling, '%Y-%m-%d %H:%M:%S')
        now = datetime.now(pytz.timezone(system_tz)).replace(tzinfo=None)
        update_freq = croniter(
            update_freq, last_tech_load_ts).get_next(datetime)

        if now >= update_freq:
            filtered_objects.append(conf)
        else:
            logging.info(
                f'''Время по частоте ещё не пришло для таблицы - {conf['schema_name']}.{conf['table_name']} ''',
            )

    return filtered_objects

# ------------------------------------------------------------------------------------------------------------------


def get_max_load_ts(config: list,
                    db_connection_src: DBConnection,
                    sql_scripts_path_select: str,
                    conf_krb_info: list) -> list:
    """
    Select из основных таблиц выборки. Забираем max(tech_load_ts)
    :param filtered_objects: лист - отфильтрованный конфиг с учётом частоты
    :param db_connection_src: объект соединения
    :param sql_scripts_path_select: путь к sql скрипт

    :return: возвращает лист - с максимальной датой(tech_load_ts) в схема-таблица
    """
    filtered_objects_with_maxdate = []

    for conf in config:
        col_name = conf.get('tech_ts_column_name')
        sql_select = get_formated_file(
            sql_scripts_path_select,
            column_name=col_name,
            schema_name=conf['schema_name'],
            table_name=conf['table_name'],
        )
        try:
            max_date = db_connection_src.apply_script_hdfs(
                sql_select, conf_krb_info)[0]
        except Exception as e:
            logging.error(
                f'''Таблица {conf['schema_name']}.{conf['table_name']} не существует или столца tech_ts нет - {e}''',
            )
            continue

        if max_date and max_date[0][0] is not None:
            conf['actual_max_tech_load_ts'] = max_date[0][0].strftime(
                '%Y-%m-%d %H:%M:%S')
            filtered_objects_with_maxdate.append(conf)

        else:
            logging.warning(
                f'''Таблица {conf['schema_name']}.{conf['table_name']} пустая или нет доступа''',
            )
    return filtered_objects_with_maxdate

# ------------------------------------------------------------------------------------------------------------------


def gen_dml(config: list,
            copy_to_vertica: str,
            delete_with_partitions: str,
            export_with_partitions: str) -> list:
    """
    Генерирует DML скрипт
    :param config: конфиг
    :param copy_to_vertica: путь к sql скрипт 
    :param delete_with_partitions: путь к sql скрипт
    :param export_with_partitions: путь к sql скрипт

    :return: возвращает конфиог с dml сриптом
    """

    conf_with_dml = []

    for conf in config:

        date_end = conf['actual_max_tech_load_ts']
        date_start = conf.get('last_date_cooling') or '1999-10-11 15:14:15'
        date_delete = (datetime.strptime(date_end, '%Y-%m-%d %H:%M:%S') -
                       timedelta(days=conf['depth'])).strftime('%Y-%m-%d %H:%M:%S')
        partition = conf.get('partition_expressions') or conf['DATE(tech_ts_column_name)']
        current_date = datetime.now().strftime('%Y%m%d')


        if conf['replication_policy'] == 1:
            if conf['cooling_type'] == 'time_based' or (conf['cooling_type'] == 'fullcopy' and date_start != '1999-10-11 15:14:15'):

                sql_export = get_formated_file(
                    export_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression=conf['filter_expression'],
                    partition_expressions=partition,
                    time_between=f'''and {conf['tech_ts_column_name']} > '{date_start}' and {conf['tech_ts_column_name']} <= '{date_end}' ''',
                    cur_date=current_date,
                )
                sql_delete = get_formated_file(
                    delete_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression=conf['filter_expression'],
                    time_between=f'''and {conf['tech_ts_column_name']} >= '{date_delete}' and {conf['tech_ts_column_name']} <= '{date_end}' '''
                )
                sql = f'{sql_export}\n{sql_delete}'

            elif conf['cooling_type'] == 'fullcopy':
                sql_export = get_formated_file(
                    export_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression='',
                    partition_expressions=partition,
                    time_between='',
                    cur_date=current_date,
                )
                sql_delete = get_formated_file(
                    delete_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression='',
                    time_between='',
                )
                sql = f'{sql_export}\n{sql_delete}'

        elif  conf['replication_policy'] == 0:
            if conf['cooling_type'] == 'time_based' or (conf['cooling_type'] == 'fullcopy' and date_start != '1999-10-11 15:14:15'):

                sql_export = get_formated_file(
                    export_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression=conf['filter_expression'],
                    partition_expressions=partition,
                    time_between=f'''and {conf['tech_ts_column_name']} > '{date_start}' and {conf['tech_ts_column_name']} <= '{date_end}' ''',
                    cur_date=current_date,
                )
                sql = f'{sql_export}'

            elif conf['cooling_type'] == 'fullcopy':
                sql_export = get_formated_file(
                    export_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression='',
                    partition_expressions=partition,
                    time_between='',
                    cur_date=current_date,
                )
                sql = f'{sql_export}'

        conf['dml_script'] = sql
        conf_with_dml.append(conf)

    return conf_with_dml

# ------------------------------------------------------------------------------------------------------------------


def run_dml(config: list, db_connection_src: DBConnection, conf_krb_info: list):
    """
    Запуск DML скриптов
    :param config: конфиг
    :param db_connection_config_src: кон к базе
    :param conf_krb_info: конфиг соединения через керберос

    :return: возвращает конфиог с dml сриптом
    """

    for conf in config:
        try:
            date_start = datetime.now()
            db_connection_src.apply_script_hdfs(
                conf['dml_script'], conf_krb_info)
            date_end = datetime.now()
            logging.info(
                f'''Продолжительность выполнения - {date_end - date_start} ''',
            )
        except Exception as e:
            logging.error(
                f'''Таблица - {conf['schema_name']}.{conf['table_name']} - не будет реплицироваться, ошибка - {e}''',
            )
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
    delete_with_partitions = conf['auxiliary_sql_paths']['sql_delete_with_partitions']
    export_with_partitions = conf['auxiliary_sql_paths']['sql_export_with_partitions']
    get_max_tech_load_ts = conf['auxiliary_sql_paths']['sql_get_max_tech_load_ts']

    con_type = conf['source_system']['system_type']
    source_type = conf['replication_objects_source']['source_type']
    source_config = conf['replication_objects_source']['source_config']
    system_tz = conf['source_system']['system_config']['system_tz']

    conf_krb_info = conf['target_system']['system_config']['connection_config']['connection_conf']

    db_connection_config_src = {
        'host': 's001cd-db-vr01.dev002.local',
        'port': '5433',
        'database': 'devdb',
        'user': 'a001cd-etl-vrt-hdp',
        "autocommit": True,
    }

    'Step 1 - создание conn к vertica'
    db_connection_src = get_connect_manager(con_type, db_connection_config_src)

    'Step 2 - берём конфиг'
    config_manager = get_config_manager(source_type, source_config)
    config = config_manager.get_config()
    logging.info(config)

    'Step 3 - Проверка конфига'
    config_check = []
    for conf in config:
        if chconf(conf):
            config_check.append(conf)
    logging.info(config_check)

    'Step 4 - фильтруем по частоте'
    filter_object = filter_objects(config_check, system_tz)
    logging.info(filter_object)

    'Step 5 - вывод кол. таблиц в конфиге'
    logging.info(
        f'''Колличество таблиц которое будеи охлаждаться - {len(filter_object)} ''')

    'Step 6 - текущая макс дата в проде'
    max_tech_load_ts = get_max_load_ts(
        filter_object, db_connection_src, get_max_tech_load_ts, conf_krb_info)
    logging.info(max_tech_load_ts)

    'Step 7 - генераия dml скриптов'
    gen_dmls = gen_dml(max_tech_load_ts, copy_to_vertica,
                       delete_with_partitions, export_with_partitions)
    logging.info(gen_dmls)

    'Step 8 - запусе dml скриптов'
    run_dml(gen_dmls, db_connection_src, conf_krb_info)
