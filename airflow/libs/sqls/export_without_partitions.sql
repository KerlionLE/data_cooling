-- Выгружаем данные из вертики в hdfs as-is (parquet)
EXPORT TO PARQUET(directory='webhdfs:///data/vertica/ODS_LEAD_GEN_TST/KW_WORD_ENTITY_FRAME_TST', compression='snappy') AS
SELECT * FROM ODS_LEAD_GEN.KW_WORD_ENTITY_FRAME
WHERE 1=1;
