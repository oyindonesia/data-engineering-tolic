SELECT
    *
from
    pg.{psql_schema}.{psql_table}
where
    true
    and id >= $min_id
    and id <= $max_id
    and created >= $psql_dstart
    and created < $psql_dend

union all

SELECT
    *
from
    pg.{psql_schema}.{psql_table}
where
    true
    and id >= $min_id
    and id <= $max_id
    and last_updated >= $psql_dstart
    and last_updated < $psql_dend
