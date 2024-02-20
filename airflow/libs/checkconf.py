import logging

from croniter import croniter


def chconf(conf: list) -> None:
    """
    Проверка каждого конфыига на входные данные
    :param conf: конфиг со всеми таблицами

   :return: True или False взависемоти от проверки конфига
    """

    # schema_name и table_name
    if isinstance(conf['schema_name'], str) is False or isinstance(conf['table_name'], str) is False:
        logging.error(
                'Поля schema_name или table_name должны быть в формате строки',
            )
        return False

    elif not conf['schema_name'] or not conf['table_name']:
        logging.error(
                f'''Поля schema_name или table_name пустые''',
            )
        return False

    # cooling_type
    if conf['cooling_type'] == 'time_based' or conf['cooling_type'] == 'fullcopy':
        logging.error(
                'Неправельно заполнено поле cooling_type - существует 2 типа - time_based, fullcopy',
            )
        return False

    # replication_policy
    if isinstance(conf['replication_policy'], int) is False:
        logging.error(
                'Неправельно заполнено поле replication_policy - должн быть в формате числа',
            )
        return False

    elif conf['replication_policy'] is None:
        logging.error(
                'Неправельно заполнено поле replication_policy - не может быть пустым',
            )
        return False

    #depth
    if isinstance(conf['depth'], int) is False:
        logging.error(
                'Неправельно заполнено поле depth - должн быть в формате числа',
            )
        return False
 
    elif not conf['depth']:
        logging.error(
                'Неправельно заполнено поле depth - не может быть пустым',
            )
        return False

    # last_date_cooling
    # try:
    #    datetime.datetime.strptime(conf['last_date_cooling'], '%Y-%m-%d %H:%M:%S')  
    # except Exception:
    #     logging.error(
    #             f'''Неправельный формат времени''',
    #         )
    #     return False

    #data_cooling_frequency
    if croniter.is_valid(conf['data_cooling_frequency']) is False:
        logging.error(
                'Неправельный формат кроны''',
            )
        return False

    elif conf['data_cooling_frequency'] is None:
        logging.error(
                f''' Репликация таблицы - {conf['schema_name']}.{conf['table_name']}, не будет выполнена, так как в config нет указание шедулера ''',
            )
        return False

    #tech_ts_column_name
    if isinstance(conf['tech_ts_column_name'], str) is False:
        logging.error(
                f'''Поля tech_ts_column_name должно быть в формате строки''',
            )
        return False

    return True
