/**********************************************************************************************
the query can check wlm whether have spell throught is_diskbased, blocks_to_disk and max_blocks_to_disk fields
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

