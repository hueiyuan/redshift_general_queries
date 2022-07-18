# redshift_queries
the repo includes general queres in redshift. Like spectrum, WLM, query duration, etc.

```
|-- src/
    |-- materialized_views/
    |-- spectrum/
    |-- check_running_queue_resource.sql
    |-- check_wlm_spill.sql
    |-- copy_query_performance.sql
    |-- running_top_queries.sql
    |-- show_manual_wlm_settings.sql
    |-- table_infomation.sql
    |-- wlm_queue_resource_usage_hourly.sql
```

1. `materialized_views/`: include materialized view operation sql files.
2. `spectrum/`: include redshift spectrum operation sql files. like external schema, external table, etc.
3. check_running_queue_resource.sql: check queue of the wlm current usage resource about disk, RAM, etc.
4. check_wlm_spill.sql: check wlm execute query whether disk spill.
5. copy_query_performance.sql: check copy data from s3 performance.
6. running_top_queries.sql: show top running long queries.
7. show_manual_wlm_settings.sql: show all manual wlm settings.
8. table_infomation.sql: show all tables information. like disk, diststyle, etc.
9. wlm_queue_resource_usage_hourly.sql: hourly show all queues of the wlm resource usage.
