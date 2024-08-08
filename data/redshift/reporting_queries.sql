Analysis and Findings

SQL Queries and Results
1. Subscribers Count:
    
    SELECT channel_name, subscribers
    FROM channel_details;
    

2. Videos Published
    
    SELECT channelname, videocount
    FROM channel_details;
    

3. Publishing Trend
    
    SELECT c.channelname, TO_CHAR(DATE_TRUNC('month', publishdate), 'Month YYYY') AS month, COUNT(v.videoid) AS videocount
    FROM video_details v
    JOIN channel_details c ON v.channelid = c.channelid
    WHERE v.publishdate >= DATEADD(month, -12, CURRENT_DATE)
    GROUP BY c.channelname, DATE_TRUNC('month', v.publishdate)
    ORDER BY c.channelname, month;
    

4. Most Viewed Videos
    
    SELECT DISTINCT c.channelname, pvd.title, pvd.viewcount, pvd.publishdate
    FROM popular_video_details pvd
    JOIN channel_details c ON pvd.channelid = c.channelid
    ORDER BY viewcount DESC;
    

5. Channel Comparison
    
    SELECT c.channelname, AVG(viewcount) AS avg_views_per_video
    FROM channel_details c
    JOIN video_details v ON v.channelid = c.channelid
    GROUP BY c.channelname
    ORDER BY avg_views_per_video DESC;
    

6. Outliers and Anomalies
    
    SELECT DISTINCT c.channelname, v.title, v.viewcount
    FROM channel_details c
    JOIN video_details v ON v.channelid = c.channelid
    WHERE v.viewcount > (SELECT AVG(viewcount) + 2 * STDDEV(viewcount) FROM video_details)
    ORDER BY v.viewcount DESC;
