-- Загружаем данные назад в Vertica
COPY {schema_name}.{table_name} FROM 'webhdfs:///data/vertica/{schema_name}/{table_name}/tech_hdfs_load_ts=20240213/*' PARQUET(do_soft_schema_match_by_name='True');