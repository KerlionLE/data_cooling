-- Выгружаем данные из вертики в hdfs с партицированием "на лету" (parquet)
EXPORT TO PARQUET(directory='webhdfs:///data/vertica/{schema_name}/{table_name}T_{current_date}', compression='snappy') OVER(PARTITION BY part) AS 
SELECT *, {partition_expressions} as part 
FROM {schema_name}.{table_name} 
WHERE 1=1 {filter_expression} 
limit 100000;