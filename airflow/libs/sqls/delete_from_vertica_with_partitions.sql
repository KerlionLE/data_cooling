DROP TABLE IF EXISTS {schema_name}.{table_name}{postfix};

CREATE TABLE {schema_name}.{table_name}{postfix} LIKE {schema_name}.{table_name} INCLUDING PROJECTIONS;

INSERT INTO {schema_name}.{table_name}{postfix}
SELECT *
FROM {schema_name}.{table_name}
WHERE 1=1 {time_between};

TRUNCATE TABLE {schema_name}.{table_name};

SELECT copy_table('{schema_name}.{table_name}{postfix}','{schema_name}.{table_name}');

DROP TABLE {schema_name}.{table_name}{postfix}