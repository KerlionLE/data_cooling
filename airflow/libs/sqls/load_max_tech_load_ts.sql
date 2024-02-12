INSERT INTO {schema_table_name_registry}(schema_name, table_name, last_tech_load_ts)
VALUES (
        '{schema_name}',
        '{table_name}',
        '{actual_max_tech_load_ts}'
    );