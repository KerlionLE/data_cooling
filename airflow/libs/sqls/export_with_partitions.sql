-- Выгружаем данные из вертики в hdfs с партицированием "на лету" (parquet)
EXPORT TO PARQUET(directory='webhdfs:///data/vertica_cold_data/{schema_name}/{table_name}/tech_hdfs_load_ts={cur_date}', compression='snappy') OVER(PARTITION BY part) AS 
SELECT *, {partition_expressions} as part 
FROM {schema_name}.{table_name} 
WHERE 1=1 {filter_expression} {time_between};