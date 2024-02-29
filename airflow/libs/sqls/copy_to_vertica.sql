-- Загружаем данные назад в Vertica
COPY {schema_name}.{table_name} FROM 'webhdfs:///data/vertica/{schema_name}/{table_name}/*/*/*.parquet' PARQUET(do_soft_schema_match_by_name='True');