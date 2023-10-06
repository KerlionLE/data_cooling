-- Выгружаем данные из вертики в hdfs с партицированием "на лету" (parquet)
EXPORT TO PARQUET(directory='webhdfs:///data/vertica/ODS_LEAD_GEN_TST/KW_WORD_ENTITY_FRAME_TST_PART', compression='snappy')
OVER(PARTITION BY part) AS
SELECT word, entity_index, tech_load_ts, is_deleted, tech_job_id, substr(entity_index, 1, 1) as part
FROM ODS_LEAD_GEN.KW_WORD_ENTITY_FRAME
WHERE 1=1;