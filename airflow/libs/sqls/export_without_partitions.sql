-- Выгружаем данные из вертики в hdfs as-is (parquet)
EXPORT TO PARQUET(directory='webhdfs:///data/vertica/{schema_name}/KW_WORD_ENTITY_FRAME_TST', compression='snappy') AS 
SELECT * FROM {schema_name}.{table_name} 
WHERE 1=1 {filter_expression} 
limit 100000;