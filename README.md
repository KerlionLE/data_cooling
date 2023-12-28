# Config

"TECH_DATA_COOLING": {
    "DEV": {
      "data_cooling": {
        "workers": 2, - кол. воркеров в airflow
        "source_system": {
            "system_type": "vertica", - тип системы для класса
            "system_config": {
                "connection_config": {
                    "connection": "airflow_conn", - тип конекшина к системе 
                    "connection_conf": {
                        "conn_id":  "vertica_staging" - кон id, который есть в airflow connection
                    }
                },
                "system_tz": "Europe/Moscow" - часовой пояс, для правильного data_cooling_frequency
            }
        },
        "target_system": {
            "system_type": "hdfs", - тип системы
            "system_config": {
                "connection_config": {
                    "connection": "airflow_conn", - тип конекшина к системе 
                    "connection_conf": {
                      "principal": "a001cd-etl-vrt-hdp@DEV002.LOCAL", - principal пользователь на которого есть доступ через kerberos в hdfs и airflow 
                      "keytab": "/usr/local/airflow/data/data_cooling/vrt_hdp.keytab" - путь/файл для генерации кейтаба, каждый раз когда выполняется скрипт - нов.
                    }
                },
                "system_tz": "Europe/Moscow" - часовой пояс, для правильного data_cooling_frequency
            }
        },
        "replication_objects_source": { - список конфигов для охлаждения
          "source_type": "json", - тип когфига, для класса 
          "source_config": [
            {
              "schema_name": "ODS_LEAD_GEN", - название схемы
              "table_name": "KW_WORD_ENTITY_FRAME", - название таблицы
              "cooling_type": "time_based", - тип охлаждения time_based(export с фильтрами )/fullcopy(польное охлаждение)
              "replication_policy": 1, - 1 или 2 для прнимания нужно ли удлалять данные после охлаждения из прода
              "depth": 123, - глубина хранения данных 
              "last_date_cooling": "2023-10-13 12:23:12", - пследняя дата охлаждения, достаётся из data_cataloga
              "data_cooling_frequency": "*/1 * * * *", - частота охлаждения(крона)
              "tech_ts_column_name": "tech_load_ts", - поле для понимания макс даты в таблицы (так как тех поле с датой может отличаться)
              "filter_expression": "and name != 'Bob' and int = 12 and", - поля для фильтров 
              "partition_expressions": "substr(entity_index, 1, 1)" - поля партиции используется в экспорте
            }
          ]
        },
        "registry": {
            "registry_type": "vertica", - тип базы, где храниться техническая таблица
            "registry_config": {
                "schema_name": "AUX_REPLICATION", - имя схемы 
                "table_name": "REPLICATION_TABLE" - имя таблицы
            }
        },
        "auxiliary_sql_paths": { - вспомогательные поля с путями к скриптам 
          "sql_export_without_partitions": "$AIRFLOW_HOME/libs/data_cooling/sqls/export_without_partitions.sql", - скрипт экспорта без партициями
          "sql_export_with_partitions": "$AIRFLOW_HOME/libs/data_cooling/sqls/export_with_partitions.sql", - скрипт эскпорта с ппартициями
          "sql_copy_to_vertica": "$AIRFLOW_HOME/libs/data_cooling/sqls/copy_to_vertica.sql", - скрипт copy данных
          "sql_delete_without_partitions": "$AIRFLOW_HOME/libs/data_cooling/sqls/delete_from_vertica_without_partitions.sql", - удаление из прода без партиций
          "sql_delete_with_partitions": "$AIRFLOW_HOME/libs/data_cooling/sqls/delete_from_vertica_with_partitions.sql", - удаление из прода с партиций
          "sql_get_max_tech_load_ts": "$AIRFLOW_HOME/libs/data_cooling/sqls/get_max_tech_load_ts.sql" - скрипт - берём макс дату из таблиц для охлаждения из прода
        }
      }
    }
  }