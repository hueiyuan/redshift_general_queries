/**********************************************************************************************
Returns the per-hour Resources usage per queueof WLM for the past 2 days. 
These results can be used to fine tune WLM queues and find peak times for workload.
    
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

SELECT date_trunc('hour', convert_timezone('utc','utc',w.exec_start_time)) AS exec_hour, 
        w.service_clASs AS "Q", 
        sum(decode(w.final_state, 'Completed',1,'Evicted',0,0)) AS n_cp,  
        sum(decode(w.final_state, 'Completed',0,'Evicted',1,0)) AS n_ev, 
        avg(w.total_queue_time/1000000) AS avg_q_sec, 
        avg(w.total_exec_time/1000000) AS avg_e_sec,
        avg(m.query_cpu_usage_percent) AS avg_pct_cpu, 
        max(m.query_cpu_usage_percent) AS max_pct_cpu, 
        max(m.query_temp_blocks_to_disk) AS max_spill, 
        sum(m.query_temp_blocks_to_disk) AS sum_spill_mb, 
        sum(m.scan_row_count) AS sum_row_scan, 
        sum(m.join_row_count) AS sum_join_rows, 
        sum(m.nested_loop_join_row_count) AS sum_nl_join_rows, 
        sum(m.return_row_count) AS sum_ret_rows, 
        sum(m.spectrum_scan_size_mb) AS sum_spec_mb
FROM  stl_wlm_query AS w LEFT JOIN svl_query_metrics_summary AS m using (userid,service_ClASs, query)
WHERE service_clASs > 5  AND  w.exec_start_time >=  dateadd(day, -1, current_Date) GROUP BY 1,2 
UNION ALL
SELECT date_trunc('hour', convert_timezone('utc','utc',c.starttime)) AS exec_hour, 
        0 AS "Q", 
        sum(decode(c.aborted, 1,0,1)) AS n_cp,  
        sum(decode(c.aborted, 1,1,0)) AS n_ev, 
        0 AS avg_q_sec, 
        avg(c.elapsed/1000000) AS avg_e_sec,
        0 AS avg_pct_cpu, 
        0 AS max_pct_cpu, 
        0 AS max_spill, 
        0 AS sum_spill_mb, 
        0 AS sum_row_scan, 
        0 AS sum_join_rows, 
        0 AS sum_nl_join_rows, 
        sum(m.return_row_count) AS sum_ret_rows, 
        0 AS sum_spec_mb
FROM svl_qlog c LEFT JOIN svl_query_metrics_summary AS m ON ( c.userid = m.userid AND c.source_query=m.query ) 
WHERE source_query IS NOT NULL AND c.starttime >=  dateadd(day, -1, current_Date)
GROUP BY 1,2  
ORDER BY  1 
DESC,2 ;
