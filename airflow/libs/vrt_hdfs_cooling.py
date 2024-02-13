import logging
import pytz

from datetime import datetime, timedelta
from croniter import croniter

from .connect_manager import DBConnection
from .utils import get_formated_file, get_connect_manager, get_config_manager
from .checkconf import chconf

# ------------------------------------------------------------------------------------------------------------------


def get_last_tech_load_ts(schemas: list,
                          tables: list,
                          schema_table_name_registry: str,
                          db_connection_src: DBConnection,
                          sql_scripts_path: str,
                          conf_krb_info) -> dict:
    """
    Забираем max дату из технической таблицы - записываем в словарь с 2-мя ключами
    :param schemas: название схем, которые нужно реплицировать
    :param tables: название таблиц, которые нужно реплицировать
    :param schema_table_name_registry: название технической схемы-таблицы с историей работы репликации
    :param db_connection_src: объект соединения
    :param sql_scripts_path: путь к sql скрипту

    :return: возвращает словарь с 2-мя ключами - схема, таблица
    """
    sql = get_formated_file(
        sql_scripts_path,
        schema_table_name=schema_table_name_registry,
        schema_names=', '.join("'" + element + "'" for element in schemas),
        table_names=', '.join("'" + element + "'" for element in tables),
    )
    return {
        (schema_name, table_name): {'tech_load_ts': tech_load_ts}
        for schema_name, table_name, tech_load_ts in db_connection_src.apply_script_hdfs(sql, conf_krb_info)[0]}

# ------------------------------------------------------------------------------------------------------------------


def filter_objects(config: dict, system_tz: str, objects) -> list:
    """
    Фильтрует словарь относительно частоты загрузки данных, сравнивая его с config timezone - airflow в utc, вертика в utc +3
    :param config: конфиг репликации
    :param system_tz: таймзона в конфиге

    :return: возвращает лист - отфильтрованный конфиг с учётом частоты
    """
    filtered_objects = []
    for conf in config:

        db_data = objects.get(
            (conf['schema_name'], conf['table_name']))  # для тестов
        last_date_cooling = conf.get('last_date_cooling')
        update_freq = conf.get('data_cooling_frequency')

        if not db_data:
            conf['is_new'] = True
            conf['last_tech_load_ts'] = None
            filtered_objects.append(conf)
            continue

        conf['is_new'] = False
        # last_tech_load_ts = datetime.strptime(
        # last_date_cooling, '%Y-%m-%d %H:%M:%S')
        last_tech_load_ts = db_data['tech_load_ts'].replace(tzinfo=None)
        conf['last_date_cooling'] = last_tech_load_ts.strftime(
            '%Y-%m-%d %H:%M:%S')
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

        actual_max_tech_load_ts = conf['actual_max_tech_load_ts']
        depth_cooling = conf['depth']
        tech_ts_column_name = conf.get('tech_ts_column_name') or 'tech_load_ts'

        temporary_heating = conf.get('temporary_heating') or False

        date_start = conf.get('last_date_cooling') or '1000-10-01 15:14:15'
        partition = conf.get(
            'partition_expressions') or f'''DATE({tech_ts_column_name})'''

        current_date = datetime.now()
        date_end_cooling_depth = (datetime.strptime(
            actual_max_tech_load_ts, '%Y-%m-%d %H:%M:%S') - timedelta(days=depth_cooling)).strftime('%Y-%m-%d %H:%M:%S')

        sql_export_date_start_date_end_cooling_depth = get_formated_file(
            export_with_partitions,
            schema_name=conf['schema_name'],
            table_name=conf['table_name'],
            filter_expression=conf['filter_expression'],
            partition_expressions=partition,
            time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{date_end_cooling_depth}' ''',
            cur_date=(datetime.now() - timedelta(days=7)).strftime('%Y%m%d'),
        )

        sql_delete_date_start_date_end_cooling_depth = get_formated_file(
            delete_with_partitions,
            schema_name=conf['schema_name'],
            table_name=conf['table_name'],
            filter_expression=conf['filter_expression'],
            time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{date_end_cooling_depth}' '''
        )

        if conf['cooling_type'] == 'time_based':
            if conf['replication_policy'] == 1:
                if temporary_heating:

                    depth_heating = conf['temporary_heating']['depth']
                    date_end_heating_depth = (datetime.strptime(
                        actual_max_tech_load_ts, '%Y-%m-%d %H:%M:%S') - timedelta(days=depth_heating)).strftime('%Y-%m-%d %H:%M:%S')
                    date_end_heating = datetime.strptime(
                        conf['temporary_heating']['date_end'], '%Y-%m-%d %H:%M:%S')
                    date_start_heating = datetime.strptime(
                        conf['temporary_heating']['date_start'], '%Y-%m-%d %H:%M:%S')

                    sql_delete_date_start_date_end_heating_depth = get_formated_file(
                        delete_with_partitions,
                        schema_name=conf['schema_name'],
                        table_name=conf['table_name'],
                        filter_expression=conf['filter_expression'],
                        time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{date_end_heating_depth}' '''
                    )
                    print("------1-------")

                    if conf['temporary_heating']['already_heat'] == 0 and current_date >= date_start_heating and current_date < date_end_heating:
                        sql_copy_to_vertica = get_formated_file(
                            copy_to_vertica,
                            schema_name=conf['schema_name'],
                            table_name=conf['table_name'],
                            cur_date=(datetime.now() - timedelta(days=7)).strftime('%Y%m%d'),
                        )
                        print("------2-------")

                        sql = f'{sql_copy_to_vertica}\n{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_heating_depth}'

                    elif conf['temporary_heating']['already_heat'] == 1 and current_date >= date_start_heating and current_date < date_end_heating:
                        print("------2.1-------")
                        sql = f'{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_heating_depth}'

                    elif conf['temporary_heating']['already_heat'] == 1 and current_date > date_end_heating:
                        print("------2.2-------")
                        sql = f'{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_cooling_depth}'

                else:
                    print("------3-------")
                    sql = f'{sql_export_date_start_date_end_cooling_depth}\n{sql_delete_date_start_date_end_cooling_depth}'

            elif conf['replication_policy'] == 0:
                print("------4-------")
                sql = f'{sql_export_date_start_date_end_cooling_depth}'

        elif conf['cooling_type'] == 'fullcopy':
            if conf['replication_policy'] == 0:
                print("------5-------")
                sql_export = get_formated_file(
                    export_with_partitions,
                    schema_name=conf['schema_name'],
                    table_name=conf['table_name'],
                    filter_expression='',
                    partition_expressions=partition,
                    time_between=f'''and {tech_ts_column_name} > '{date_start}' and {tech_ts_column_name} <= '{actual_max_tech_load_ts}' ''',
                    cur_date=(datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
                )
                sql = f'{sql_export}'

        conf['dml_script'] = sql
        conf_with_dml.append(conf)

    return conf_with_dml

# ------------------------------------------------------------------------------------------------------------------


def run_dml(config: list, db_connection_src: DBConnection, conf_krb_info: list, load_max_tech_load_ts_insert, schema_table_name_registry):
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

            sql_insert = get_formated_file(
                load_max_tech_load_ts_insert,
                schema_table_name_registry=schema_table_name_registry,
                schema_name=conf['schema_name'],
                table_name=conf['table_name'],
                actual_max_tech_load_ts=conf['actual_max_tech_load_ts'],
            )
            db_connection_src.apply_script_hdfs(
                sql_insert, conf_krb_info)
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
    :param conf: конфиг запуска охлаждения
    :param db_connection_config_src: config src con

    """

    copy_to_vertica = conf['auxiliary_sql_paths']['sql_copy_to_vertica']
    delete_with_partitions = conf['auxiliary_sql_paths']['sql_delete_with_partitions']
    export_with_partitions = conf['auxiliary_sql_paths']['sql_export_with_partitions']
    get_max_tech_load_ts = conf['auxiliary_sql_paths']['sql_get_max_tech_load_ts']

    # Тесты
    get_last_tech_load_ts_sql = conf['auxiliary_sql_paths']['get_last_tech_load_ts']
    # Тесты
    load_max_tech_load_ts_insert = conf['auxiliary_sql_paths']['load_max_tech_load_ts_insert']

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

    'Step FOR TEST - берём макс дату последней репликации'
    tables = [el['table_name'] for el in config_check]
    schemas = [el['schema_name'] for el in config_check]
    schema_table_name_registry = 'AUX_REPLICATION.COOLING_TABLE'
    last_tech_load_ts = get_last_tech_load_ts(
        schemas, tables, schema_table_name_registry, db_connection_src, get_last_tech_load_ts_sql, conf_krb_info)
    logging.info(last_tech_load_ts)

    'Step 4 - фильтруем по частоте'
    filter_object = filter_objects(config_check, system_tz, last_tech_load_ts)
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
    run_dml(gen_dmls, db_connection_src, conf_krb_info,
            load_max_tech_load_ts_insert, schema_table_name_registry)

    'Step FOR TEST - загрузка данных в тех таблицу'
