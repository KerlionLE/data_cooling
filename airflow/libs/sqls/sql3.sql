-- Загружаем данные назад в Vertica
COPY ODS_LEAD_GEN.KW_WORD_ENTITY_FRAME_HDP FROM 'webhdfs:///data/vertica/ODS_LEAD_GEN_TST/KW_WORD_ENTITY_FRAME_TST/*.parquet' PARQUET;