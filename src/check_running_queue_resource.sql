/**********************************************************************************************
Return the Current queries running and queueing, along with resource consumption.

Columns:
user :			User name
pid :			Pid of the session
xid :			Transaction identity
query :			Query Id
q :				Queue
slt :			Slots Uses
start :			Time query was issued
state :			Current State
q_sec :			Seconds in queue
exe_sec :		Seconds Executed
cpu_sec :		CPU seconds consumed
read_mb :		MB read by the query
spill_mb :		MB spilled to disk
ret_rows :		Rows returned to Leader -> Client
nl_rows :		# of rows of Nested Loop Join
sql :			First 90 Characters of the query SQL
alert :			Alert events related to the query
**********************************************************************************************/

SELECT trim(u.usename) AS user,
        s.pid, 
        q.xid,
        q.query,
        q.service_clASs AS "q", 
        q.slot_count AS slt,
        date_trunc('second',q.wlm_start_time) AS start,
        decode(trim(q.state), 'Running','Run','QueuedWaiting','Queue','Returning','Return',trim(q.state)) AS state, 
        q.queue_Time/1000000 AS q_sec, 
        q.exec_time/1000000 AS exe_sec,
        m.cpu_time/1000000 cpu_sec, 
        m.blocks_read read_mb, 
        decode(m.blocks_to_disk,-1,null,m.blocks_to_disk) spill_mb, m2.rows AS ret_rows, m3.rows AS NL_rows,
        substring(replace(nvl(qrytext_cur.text,trim(translate(s.text,chr(10)||chr(13)||chr(9) ,''))),'\\n',' '),1,90) AS sql,
        trim(decode(event&1,1,'SK ','') || decode(event&2,2,'Del ','') || decode(event&4,4,'NL ','') ||  decode(event&8,8,'Dist ','') || decode(event&16,16,'BcASt ','') || decode(event&32,32,'Stats ','')) AS Alert
FROM  stv_wlm_query_state q 
LEFT OUTER JOin stl_querytext s ON (s.query=q.query AND sequence = 0)
LEFT OUTER JOin stv_query_metrics m ON ( q.query = m.query AND m.segment=-1 AND m.step=-1 )
LEFT OUTER JOin stv_query_metrics m2 ON ( q.query = m2.query AND m2.step_type = 38 )
LEFT OUTER JOin ( 
    SELECT query, sum(rows) AS rows 
    FROM stv_query_metrics m3 
    WHERE step_type = 15 
    GROUP BY 1
) AS m3 ON ( q.query = m3.query )
LEFT OUTER JOin pg_user u ON ( s.userid = u.usesysid )
LEFT OUTER JOin (
    SELECT ut.xid,'CURSOR ' || TRIM( substring ( TEXT FROM strpos(upper(TEXT),'SELECT') )) AS TEXT
    FROM stl_utilitytext ut
    WHERE sequence = 0 AND upper(TEXT) LIKE 'DECLARE%'
    GROUP BY text, ut.xid
) qrytext_cur ON (q.xid = qrytext_cur.xid)
LEFT OUTER JOin ( 
    SELECT query,sum(decode(trim(split_part(event,':',1)),'Very SELECTive query filter',1,'Scanned a large number of deleted rows',2,'Nested Loop Join in the query plan',4,'Distributed a large number of rows across the network',8,'BroadcASted a large number of rows across the network',16,'Missing query planner statistics',32,0)) AS event 
    FROM STL_ALERT_EVENT_LOG 
    WHERE event_time >=  dateadd(hour, -8, current_Date) GROUP BY query  
) AS alrt ON alrt.query = q.query
order by q.service_clASs,q.exec_time DESC, q.wlm_start_time;
