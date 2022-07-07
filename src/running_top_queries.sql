/**********************************************************************************************
Return the top 50 time consuming statements aggregated by it's text in past 7 days.

Columns:
   exec_hour: 		Hour of execution of queries
   q: 			ID for the service class, defined in the WLM configuration file. 
   n_cp:		Number of queries executed on that queue/hour
   avg_q_sec:		Average Queueing time in seconds
   avg_e_sec:		Averagte Executiong time in seconds
   avg_pct_cpu:		Average percentage of CPU used by the query. Value can be more than 100% for multi-cpu/slice systems
   max_pct_cpu:		Max percentage of CPU used by the query. Value can be more than 100% for multi-cpu/slice systems
   sum_spill_mb:	Sum of Spill usage by that queue on that hour
   sum_row_scan:	Sum of rows scanned on that queue/hour
   sum_join_rows:	Sum of rows joined on that queue/hour
   sum_nl_join_rows:	Sum of rows Joined using Nested Loops on that queue/hour
   sum_ret_rows:	Sum of rows returned to the leader/client on that queue/hour
   sum_spec_mb:		Sum of Megabytes scanned by a Spectrum query on that queue/hour
**********************************************************************************************/

select trim(database) as DB, 
        count(query) as n_qry, 
        max(substring (qrytext,1,80)) as qrytext, 
        min(run_seconds) as "min" , 
        max(run_seconds) as "max", 
        avg(run_seconds) as "avg", 
        sum(run_seconds) as total,  
        max(query) as max_query_id, 
        max(starttime)::date as last_run, 
        aborted,
        listagg(event, ', ') within group (order by query) as events
from (
    select userid,
            label,
            stl_query.query,
            trim(database) as database,
            trim(querytxt) as qrytext,
            md5(trim(querytxt)) as qry_md5,
            starttime, endtime, 
            datediff(seconds, starttime,endtime)::numeric(12,2) as run_seconds, 
            aborted, 
            decode(alrt.event,'Very selective query filter','Filter','Scanned a large number of deleted rows','Deleted','Nested Loop Join in the query plan','Nested Loop','Distributed a large number of rows across the network','Distributed','Broadcasted a large number of rows across the network','Broadcast','Missing query planner statistics','Stats',alrt.event) as event
    from stl_query 
    left outer join ( 
        select query, trim(split_part(event,':',1)) as event 
        from STL_ALERT_EVENT_LOG 
        where event_time >=  dateadd(day, -7, current_Date)  
        group by query, trim(split_part(event,':',1)) 
    ) as alrt on alrt.query = stl_query.query
    where userid <> 1 
    -- and (querytxt like 'SELECT%' or querytxt like 'select%' ) 
    -- and database = ''
    and starttime >=  dateadd(day, -7, current_Date)
) 
group by database, label, qry_md5, aborted
order by total desc limit 50;
