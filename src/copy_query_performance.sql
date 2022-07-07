/*******************************************************************************
Purpose: Return COPY information from past 7 days

Columns:
Starttime:		    Date and Time COPY started
query:	    		  Query id
querytxt:         Partial SQL
n_files:		      Number of files
size_mb:	    	  Size of the COPY in Megabytes
time_seconds:		  Duration in Seconds
mb_per_s:		      Megabytes per second

*******************************************************************************/

SELECT q.starttime,
        s.query, 
        substring(q.querytxt,1,120) AS querytxt,
        s.n_files, 
        size_mb, 
        s.time_seconds,
        s.size_mb/decode(s.time_seconds,0,1,s.time_seconds)  AS mb_per_s
FROM (
    SELECT query, 
            count(*) AS n_files,
            sum(transfer_size/(1024*1024)) AS size_MB, 
            (max(end_Time) -min(start_Time))/(1000000) AS time_seconds ,
            max(end_time) AS end_time
    FROM stl_s3client 
    WHERE http_method = 'GET' AND query > 0 AND transfer_time > 0 GROUP BY query 
) AS s
LEFT JOIN stl_Query AS q ON q.query = s.query
WHERE s.end_Time >=  dateadd(day, -7, current_Date)
ORDER BY s.time_Seconds DESC, size_mb DESC, s.end_time DESC
LIMIT 50;
