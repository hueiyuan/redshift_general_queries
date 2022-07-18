/**********************************************************************************************
We can get table information from SVV_TABLE_INFO. include table disk size, tbl_rows, skew_rows, encoded, diststyle, sortkey, etc.

reference: https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html
**********************************************************************************************/

SELECT "table", size, tbl_rows, skew_rows, encoded, diststyle, sortkey1
FROM SVV_TABLE_INFO
WHERE "table"='your_table_name' 
ORDER BY "table" DESC;

