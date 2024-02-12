SELECT schema_name,
    table_name,
    max(tech_load_ts) as tech_load_ts
FROM {schema_table_name}
where schema_name in (
        {schema_names}
    )
    and table_name in (
        {table_names}
    )
group by schema_name,
    table_name