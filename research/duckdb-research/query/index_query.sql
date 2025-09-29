SELECT
    min(id) as min_id,
    max(id) as max_id
from
    pg.{psql_schema}.{psql_table}
