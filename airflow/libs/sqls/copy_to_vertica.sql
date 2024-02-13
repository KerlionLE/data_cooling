-- Загружаем данные назад в Vertica
COPY {schema_name}.{table_name} FROM 'webhdfs:///data/vertica/{schema_name}/{table_name}/*/*/*.parquet' PARQUET;