/**********************************************************************************************
Create materialized view from external table.
**********************************************************************************************/
CREATE MATERIALIZED VIEW mv_test AS SELECT
  id,
  uid,
  region,
  dt,
  hour
FROM spectrum_schema.test_table
WHERE ("region" = 'TW');

/**********************************************************************************************
Refresh materialized view.
**********************************************************************************************/
REFRESH MATERIALIZED VIEW mv_test;
