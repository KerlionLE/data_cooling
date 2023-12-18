import logging
import datetime

from croniter import croniter


def chconf(conf:list) -> None:
    """
    :param conf: конфиг

    """

    # schema_name и table_name
    if conf['schema_name'] is not str or conf['table_name'] is not str:
        logging.error(
                f'''Поля schema_name или table_name должны быть в формате строки''',
            )

    elif not conf['schema_name'] or not conf['table_name']:
        logging.error(
                f'''Поля schema_name или table_name пустые''',
            )

    # cooling_type
    if conf['cooling_type'] != 'time_based' or conf['cooling_type'] != 'fullcopy':
        logging.error(
                f'''Неправельно заполнено поле cooling_type - существует 2 типа - time_based, fullcopy''',
            )

    # replication_policy
    if conf['replication_policy'] is not int:
        logging.error(
                f'''Неправельно заполнено поле replication_policy - должн быть в формате числа''',
            )

    elif not conf['replication_policy']:
        logging.error(
                f'''Неправельно заполнено поле replication_policy - не может быть пустым''',
            )

    #depth
    if conf['depth'] is not int:
        logging.error(
                f'''Неправельно заполнено поле depth - должн быть в формате числа''',
            )
 
    elif not conf['depth']:
        logging.error(
                f'''Неправельно заполнено поле depth - не может быть пустым''',
            )

    #last_date_cooling
    try:
        datetime.datetime.strptime(conf['last_date_cooling'], '%Y-%m-%d %H:%M:%S')  
    except Exception:
        logging.error(
                f'''Неправельный формат времени''',
            )

    #data_cooling_frequency
    if croniter.is_valid('0 0 1 * *') is False:
        logging.error(
                f'''Неправельный формат кроны''',
            )

    #tech_ts_column_name
    if conf['tech_ts_column_name'] is not str:
        logging.error(
                f'''Поля tech_ts_column_name должно быть в формате строки''',
            )

    