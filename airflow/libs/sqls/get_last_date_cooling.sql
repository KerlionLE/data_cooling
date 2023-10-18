select schema_table_name, max(last_data_cooling)
from sandbox.data_cooling
where schema_table_name = '{schema_name}.{table_name}'
group by schema_table_name