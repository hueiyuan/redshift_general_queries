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

SELECT trim(databASe) AS DB, 
        count(query) AS n_qry, 
        max(substring (qrytext,1,80)) AS qrytext, 
        min(run_seconds) AS "min" , 
        max(run_seconds) AS "max", 
        avg(run_seconds) AS "avg", 
        sum(run_seconds) AS total,  
        max(query) AS max_query_id, 
        max(starttime)::date AS lASt_run, 
        aborted,
        listagg(event, ', ') WITHIN group (ORDER BY query) AS events
FROM (
    SELECT userid,
            label,
            stl_query.query,
            trim(databASe) AS databASe,
            trim(querytxt) AS qrytext,
            md5(trim(querytxt)) AS qry_md5,
            starttime, endtime, 
            datediff(seconds, starttime,endtime)::numeric(12,2) AS run_seconds, 
            aborted, 
            decode(alrt.event,'Very SELECTive query filter','Filter','Scanned a large number of deleted rows','Deleted','Nested Loop Join in the query plan','Nested Loop','Distributed a large number of rows across the network','Distributed','BroadcASted a large number of rows across the network','BroadcASt','Missing query planner statistics','Stats',alrt.event) AS event
    FROM stl_query 
    LEFT OUTER JOIN ( 
        SELECT query, trim(split_part(event,':',1)) AS event 
        FROM STL_ALERT_EVENT_LOG 
        WHERE event_time >=  dateadd(day, -7, current_Date)  
        GROUP BY query, trim(split_part(event,':',1)) 
    ) AS alrt on alrt.query = stl_query.query
    WHERE userid <> 1 
    -- AND (querytxt like 'SELECT%' or querytxt like 'SELECT%' ) 
    -- AND databASe = ''
    AND starttime >=  dateadd(day, -7, current_Date)
) 
GROUP BY databASe, label, qry_md5, aborted
ORDER BY total DESC LIMIT 50;
