/**********************************************************************************************
Create external scheam before create external table for redshift spectrum.
**********************************************************************************************/

CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_schema
FROM DATA CATALOG
DATABASE 'etl_golden_staging'
IAM_ROLE 'arn:aws:iam::account_id:role/redshift-iam-role'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


/**********************************************************************************************
Create external table for redshift spectrum with delta lake manifest.
**********************************************************************************************/

create external table spectrum_schema.test_table(
  uid varchar,
  region varchar
)
PARTITIONED BY(dt varchar, hour varchar, minute varchar)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://data-bucket/data/table/_symlink_format_manifest/';


/**********************************************************************************************
Alter table partition if manifest is updated.
**********************************************************************************************/
ALTER TABLE spectrum_schema.contacts_staging ADD IF NOT EXISTS PARTITION (dt='2022-07-12', hour='11', minute='00') 
LOCATION 's3://data-bucket/data/table/_symlink_format_manifest/dt=2022-07-12/hour=11/minute=00';

