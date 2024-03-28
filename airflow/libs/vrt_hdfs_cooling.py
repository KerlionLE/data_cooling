import logging
from datetime import datetime, timedelta

import pytz
from croniter import croniter

from data_cooling.config_manager import ConfigManager
from data_cooling.connect_manager import DBConnection
from data_cooling.utils import get_formated_file, get_connect_manager, get_config_manager


def filter_objects(config: dict, system_tz: str, hdfs_path: str) -> list:
    """
    Фильтрует словарь относительно частоты загрузки данных, сравнивая его с config timezone - airflow в utc, вертика в utc +3
    :param config: конфиг репликации
    :param system_tz: таймзона в конфиге
    :param hdfs_path: путь к данным в hdfs

    :return: возвращает лист - отфильтрованный конфиг с учётом частоты
    """
    filtered_objects = []
    for conf in config:
        conf['hdfs_path'] = f'''{hdfs_path}{conf['schema_name']}/{conf['table_name']}'''
        last_date_cooling = conf.get('last_date_cooling')
        update_freq = conf.get('data_cooling_frequency')

        if not last_date_cooling:
            conf['is_new'] = True
            conf['last_tech_load_ts'] = None
            filtered_objects.append(conf)
            continue

        conf['is_new'] = False
        last_tech_load_ts = datetime.strptime(last_date_cooling, "%Y-%m-%d %H:%M:%S")
        conf['last_date_cooling'] = last_tech_load_ts
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


def get_max_load_ts(config: list, db_connection_src: DBConnection, sql_scripts_path_select: str, conf_krb_info: list) -> list:
    """
    Select из основных таблиц выборки. Забираем max(tech_load_ts)
    :param config: конфиг
    :param db_connection_src: объект соединения
    :param sql_scripts_path_select: путь к sql скрипт
    :param conf_krb_info: конфиг кон к керберосу

    :return: возвращает лист - с максимальной датой(tech_load_ts) в схема-таблица
    """
    filtered_objects_with_maxdate = []

    for conf in config:
        col_name = conf.get('tech_ts_column_name') or 'tech_load_ts'
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
            logging.error(
                f'''Таблица {conf['schema_name']}.{conf['table_name']} пустая или нет доступа''',
            )
    return filtered_objects_with_maxdate

# ------------------------------------------------------------------------------------------------------------------


def gen_dml(config: list, copy_to_vertica: str, delete_with_partitions: str, export_with_partitions: str, hdfs_path_con: str) -> list:
    """
    Генерирует DML скрипт
    :param config: конфиг
    :param copy_to_vertica: путь к sql скрипт
    :param delete_with_partitions: путь к sql скрипт
    :param export_with_partitions: путь к sql скрипт
    :param hdfs_path_con: путь схранения данных в HDFS

    :return: возвращает конфиог с dml сриптом
    """

    conf_with_dml = []

    for conf in config:

        actual_max_tech_load_ts = conf['actual_max_tech_load_ts']
        depth_cooling = conf.get('cooling_depth') or 0
        tech_ts_column_name = conf.get('tech_ts_column_name') or 'tech_load_ts'
        filter_expression = conf.get('filter_expression') or ''

        date_start = conf.get('last_date_cooling') or '1000-10-01 15:14:15'
        partition = conf.get('partition_expressions') or f'DATE({tech_ts_column_name})'
        date_format = '%Y-%m-%d %H:%M:%S'
        sql = ''

        current_date = datetime.now()
        date_end_cooling_depth = (datetime.strptime(actual_max_tech_load_ts, date_format) - timedelta(days=depth_cooling)).strftime(date_format)

        conf['date_end_cooling_depth'] = date_end_cooling_depth

        sql_export_date_start_date_end_cooling_depth = get_formated_file(
            export_with_partitions,
            hdfs_path=hdfs_path_con,
            schema_name=conf['schema_name'],
            table_name=conf['table_name'],
            filter_expression=filter_expression,
            partition_expressions=partition,
            time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{date_end_cooling_depth}' ''',
            cur_date=(datetime.now()).strftime('%Y%m%d'),
        )

        sql_delete_date_start_date_end_cooling_depth = get_formated_file(
            delete_with_partitions,
            schema_name=conf['schema_name'],
            table_name=conf['table_name'],
            filter_expression=filter_expression,
            time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{date_end_cooling_depth}' ''',
        )

        if conf['cooling_type'] == 'TimeBased':
            if conf['replication_policy']:
                if conf['heating_type'] is not None:

                    depth_heating = conf['heating_depth']
                    date_end_heating_depth = (datetime.strptime(actual_max_tech_load_ts, date_format) - timedelta(days=int(depth_heating))).strftime(date_format)
                    date_end_heating = datetime.strptime(conf['heating_date_end'], '%Y-%m-%d %H:%M:%S%z')
                    date_start_heating = datetime.strptime(conf['heating_date_start'], '%Y-%m-%d %H:%M:%S%z')

                    sql_delete_date_start_date_end_heating_depth = get_formated_file(
                        delete_with_partitions,
                        schema_name=conf['schema_name'],
                        table_name=conf['table_name'],
                        filter_expression=filter_expression,
                        time_between=f'''and {tech_ts_column_name} <= '{date_end_heating_depth}' ''',
                    )

                    if conf['isAlreadyHeating'] == 0 and current_date >= date_start_heating.replace(tzinfo=None) and current_date < date_end_heating.replace(tzinfo=None):
                        sql_copy_to_vertica = get_formated_file(
                            copy_to_vertica,
                            hdfs_path=hdfs_path_con,
                            schema_name=conf['schema_name'],
                            table_name=conf['table_name'],
                            cur_date=(datetime.now()).strftime('%Y%m%d'),
                        )

                        sql = f'{sql_copy_to_vertica}\n{sql_delete_date_start_date_end_heating_depth}'

                    elif conf['isAlreadyHeating'] == 1 and current_date >= date_start_heating.replace(tzinfo=None) and current_date < date_end_heating.replace(tzinfo=None):
                        sql = f'{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_heating_depth}'

                    elif conf['isAlreadyHeating'] == 1 and current_date > date_end_heating.replace(tzinfo=None):
                        sql = f'{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_cooling_depth}'

                else:
                    sql = f'{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_cooling_depth}'

            elif conf['replication_policy'] is False:
                sql = f'{sql_export_date_start_date_end_cooling_depth}'

        elif conf['cooling_type'] == 'Full' and conf['replication_policy'] is False:
            sql_export = get_formated_file(
                export_with_partitions,
                hdfs_path=hdfs_path_con,
                schema_name=conf['schema_name'],
                table_name=conf['table_name'],
                filter_expression='',
                partition_expressions=partition,
                time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{actual_max_tech_load_ts}' ''',
                cur_date=(datetime.now()).strftime('%Y%m%d'),
            )
            sql = f'{sql_export}'

        conf['dml_script'] = sql
        conf_with_dml.append(conf)

    return conf_with_dml

# ------------------------------------------------------------------------------------------------------------------


def run_dml(config: list, db_connection_src: DBConnection, conf_krb_info: list) -> list:
    """
    Запуск DML скриптов
    :param config: конфиг
    :param db_connection_src: кон к базе
    :param conf_krb_info: конфиг соединения через керберос

    :return: возвращает конфиог с флагом выполненого крипта или нет
    """

    conf_data = []
    for conf in config:
        try:
            date_start = datetime.now()

            db_connection_src.apply_script_hdfs(conf['dml_script'], conf_krb_info)
            conf['is_success'] = True

            date_end = datetime.now()
            logging.info(
                f'Продолжительность выполнения - {date_end - date_start}',
            )
            conf_data.append(conf)

        except Exception as e:
            logging.error(
                f'''Таблица - {conf['schema_name']}.{conf['table_name']} - не будет реплицироваться, ошибка - {e}''',
            )

    return conf_data


def put_result(config: list, config_manager: ConfigManager) -> None:
    """
    Запуск DML скриптов
    :param config: конфиг
    :param config_manager: класс конфига

    """

    for conf in config:
        if conf['replication_policy'] and conf['dml_script'] != '':
            try:
                config_manager.put_data_cooling(conf)
            except Exception as e:
                logging.error(
                    f'''Для таблицы Таблица - {conf['schema_name']}.{conf['table_name']} - не будет записан резалт охлаждение, ошибка - {e}''',
                )
            try:
                config_manager.put_data_heating(conf)
            except Exception as e:
                logging.error(
                    f'''Для таблицы Таблица - {conf['schema_name']}.{conf['table_name']} - не будет записан резалт разогрева, ошибка - {e}''',
                )

        elif conf['replication_policy'] is False and conf['dml_script'] != '':
            try:
                config_manager.put_data_cooling(conf)
            except Exception as e:
                logging.error(
                    f'''Для таблицы Таблица - {conf['schema_name']}.{conf['table_name']} - не будет записан резалт охлаждение, ошибка - {e}''',
                )


# ------------------------------------------------------------------------------------------------------------------


def get_config_func(conf: list) -> None:
    """
    Функция забора конфига из источника
    :param conf: конфиг запуска охлаждения

    :return: возвращает конфиог
    """
    source_type = conf['replication_objects_source']['source_type']
    source_config = conf['replication_objects_source']['source_config']

    'Step 1 - Забираем конфиг из дата каталога'
    config_manager = get_config_manager(source_type, source_config)
    config = config_manager.get_config()
    logging.info(config)

    return config


def preprocess_config_cheks_con_dml_func(conf: list, db_connection_config_src: list, config: list) -> None:
    """
    Функция обработки и создания конфига
    :param conf: конфиг запуска охлаждения
    :param db_connection_config_src: креды содинения с базой
    :param config: конфиг из источника

    :return: возвращает конфиог c dml
    """

    copy_to_vertica = conf['auxiliary_sql_paths']['sql_copy_to_vertica']
    delete_with_partitions = conf['auxiliary_sql_paths']['sql_delete_with_partitions']
    export_with_partitions = conf['auxiliary_sql_paths']['sql_export_with_partitions']
    get_max_tech_load_ts = conf['auxiliary_sql_paths']['sql_get_max_tech_load_ts']

    con_type = conf['source_system']['system_type']
    system_tz = conf['source_system']['system_config']['system_tz']
    hdfs_path = conf['target_system']['system_config']['hdfs_path']

    conf_krb_info = conf['target_system']['system_config']['connection_config']['connection_conf']

    'Step 1 - класс сon'
    db_connection_src = get_connect_manager(con_type, db_connection_config_src)
    logging.info(db_connection_src)

    'Step 2 - фильтруем объекты оп частоте'
    filter_object = filter_objects(config, system_tz, hdfs_path)
    logging.info(filter_object)

    'Step 3 - берём макс дату на проде'
    max_tech_load_ts = get_max_load_ts(filter_object, db_connection_src, get_max_tech_load_ts, conf_krb_info)
    logging.info(max_tech_load_ts)

    logging.info(
        f'''Колличество таблиц которое будеи охлаждаться - {len(max_tech_load_ts)} ''')

    'Step 4 - генерим dml'
    gen_dmls = gen_dml(max_tech_load_ts, copy_to_vertica, delete_with_partitions, export_with_partitions, hdfs_path)
    logging.info(gen_dmls)

    return gen_dmls


def run_dml_func(gen_dmls: list, db_connection_config_src: list, conf: list) -> None:
    """
    Функция обработки и создания конфига
    :param gen_dmls: конфиг
    :param db_connection_config_src: креды содинения с базой
    :param conf: конфиг из источника

    :return: возвращает конфиог
    """

    con_type = conf['source_system']['system_type']
    conf_krb_info = conf['target_system']['system_config']['connection_config']['connection_conf']

    'Step 1 - класс con'
    db_connection_src = get_connect_manager(con_type, db_connection_config_src)
    logging.info(db_connection_src)

    'Step 2 - pзапускаем dml'
    return run_dml(gen_dmls, db_connection_src, conf_krb_info)


def put_result_func(config: list, conf: list) -> None:
    """
    Функция обработки и создания конфига
    :param conf: конфиг запуска охлаждения
    :param config: конфиг из источника

    """

    source_type = conf['replication_objects_source']['source_type']
    source_config = conf['replication_objects_source']['source_config']

    'Step 1 - класс конфиг'
    config_manager = get_config_manager(source_type, source_config)
    logging.info(config_manager)

    'Step 2'
    put_result(config, config_manager)
