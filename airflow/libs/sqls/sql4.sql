-- Создаем external табличку на данных в hdfs
create external table ODS_LEAD_GEN.KW_WORD_ENTITY_FRAME_HDP_EXT
(
    word         varchar(256)                               not null,
    entity_index varchar(64)                                not null,
    tech_load_ts timestamp(6) default statement_timestamp() not null,
    is_deleted   boolean      default false                 not null,
    tech_job_id  int          default 0                     not null
) as copy from 'webhdfs:///data/vertica/ODS_LEAD_GEN_TST/KW_WORD_ENTITY_FRAME_TST/*.parquet' PARQUET;