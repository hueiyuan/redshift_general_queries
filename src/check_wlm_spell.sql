/**********************************************************************************************
the query can check wlm whether have spell throught is_diskbased, blocks_to_disk and max_blocks_to_disk fields

WLM service class ids

|     ID     |                       Service Class                                  |
|------------|----------------------------------------------------------------------|
|    1-4     | Reserved for system use.                                             |
|    5       | Used by the superuser queue.                                         |
|    6-13    | Used by manual WLM queues that are defined in the WLM configuration. |
|    14      | Used by short query acceleration.                                    |
|    15      | Reserved for maintenance activities run by Amazon Redshift.          |
|   100-107  | Used by automatic WLM queue when auto_wlm is true.                   |

Reference: https://docs.aws.amazon.com/redshift/latest/dg/cm-c-wlm-system-tables-and-views.html#wlm-service-class-ids

**********************************************************************************************/

select s2.service_class, 
      s2.service_class_name, 
      s2.blocks_to_disk, 
      s2.max_blocks_to_disk, 
      s1.query, 
      s1.step, 
      s1.rows, 
      s1.workmem, 
      s1.label, 
      s1.is_diskbased
from svl_query_summary as s1
join stl_query_metrics s2 on s1.query=s2.query
order by workmem desc;

