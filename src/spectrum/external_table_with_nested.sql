/**********************************************************************************************
Create external table with nested.

note:
* An array can only contain scalars or struct types. Array types can't contain array or map types.
* Redshift Spectrum supports complex data types only as external tables.
* If the parquet data is generated from spark or delta lake, need to convert struct type if field is in ArrayType.

reference: https://docs.aws.amazon.com/redshift/latest/dg/tutorial-query-nested-data.html
**********************************************************************************************/

CREATE EXTERNAL TABLE spectrum_schema.users (
  id     int,
  name   struct<given:varchar(20), family:varchar(20)>,
  phones array<varchar(20)>,
  orders array<struct<shipdate:timestamp, price:double precision>>
)
STORED AS PARQUET
LOCATION 's3://data_bucket/nested_data/users/';


/**********************************************************************************************
if external table have complicated data type(map, array or struct). we can not select * directly from external table.
We need to specific corresponding key name.
**********************************************************************************************/

SELECT u.id, u_name.given, u_name.family
FROM spectrum_schema.users u, u.name u_name;


/* select * from external table with nested type that will be error. */
SELECT *
FROM spectrum_schema.users u, ;

