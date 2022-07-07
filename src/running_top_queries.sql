/**********************************************************************************************
Return the top 50 time consuming statements aggregated by it's text in past 7 days.
**********************************************************************************************/

select trim(database) as DB, count(query) as n_qry, max(substring (qrytext,1,80)) as qrytext, min(run_seconds) as "min" , max(run_seconds) as "max", avg(run_seconds) as "avg", sum(run_seconds) as total,  max(query) as max_query_id, 
max(starttime)::date as last_run, aborted,
listagg(event, ', ') within group (order by query) as events
from (
select userid, label, stl_query.query, trim(database) as database, trim(querytxt) as qrytext, md5(trim(querytxt)) as qry_md5, starttime, endtime, datediff(seconds, starttime,endtime)::numeric(12,2) as run_seconds, 
       aborted, decode(alrt.event,'Very selective query filter','Filter','Scanned a large number of deleted rows','Deleted','Nested Loop Join in the query plan','Nested Loop','Distributed a large number of rows across the network','Distributed','Broadcasted a large number of rows across the network','Broadcast','Missing query planner statistics','Stats',alrt.event) as event
from stl_query 
left outer join ( select query, trim(split_part(event,':',1)) as event from STL_ALERT_EVENT_LOG where event_time >=  dateadd(day, -7, current_Date)  group by query, trim(split_part(event,':',1)) ) as alrt on alrt.query = stl_query.query
where userid <> 1 
-- and (querytxt like 'SELECT%' or querytxt like 'select%' ) 
-- and database = ''
and starttime >=  dateadd(day, -7, current_Date)
 ) 
group by database, label, qry_md5, aborted
order by total desc limit 50;
