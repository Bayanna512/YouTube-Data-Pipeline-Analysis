Analysis and Findings

SQL Queries and Results
1) Subscribers Count: ?
    
    SELECT channel_name, subscribers
    FROM channel_details;
    

2) Videos Published ?
    
    SELECT channelname, videocount
    FROM channel_details;
    

3) All-Time Top 10 Most Viewed Videos: Highlighting Channel , video titles and Their Hits	?			
    
    select distinct c.channelname, pvd.title , pvd.viewcount,pvd.publishdate from popular_video_details pvd
 JOIN channel_details c ON pvd.channelid = c.channelid
 order by viewcount desc limit 10	
    

4) Avg views per video across each channel	?
    
    SELECT c.channelname, 
       AVG(viewcount) AS avg_views_per_video
FROM channel_details c
JOIN video_details v on v.channelid = c.channelid
GROUP BY c.channelname
ORDER BY avg_views_per_video DESC;			


    
5) 12-Month Channel wise View Count Dashboard	?
    
 WITH video_month_counts AS (
    SELECT
        c.channelname,
        TO_CHAR(DATE_TRUNC('month', v.publishdate), 'YYYY-MM') AS month,
        COUNT(v.videoid) AS videocount
    FROM
        video_details v
    JOIN
        channel_details c ON v.channelid = c.channelid
    WHERE
        v.publishdate BETWEEN '2023-08-11' AND '2024-08-10'
    GROUP BY
        c.channelname,
        DATE_TRUNC('month', v.publishdate)
)
SELECT 
    channelname,
    MAX(CASE WHEN month = '2024-08' THEN videocount ELSE 0 END) AS "August 2024",
    MAX(CASE WHEN month = '2024-07' THEN videocount ELSE 0 END) AS "July 2024",
    MAX(CASE WHEN month = '2024-06' THEN videocount ELSE 0 END) AS "June 2024",
    MAX(CASE WHEN month = '2024-05' THEN videocount ELSE 0 END) AS "May 2024",
    MAX(CASE WHEN month = '2024-04' THEN videocount ELSE 0 END) AS "April 2024",
    MAX(CASE WHEN month = '2024-03' THEN videocount ELSE 0 END) AS "March 2024",
    MAX(CASE WHEN month = '2024-02' THEN videocount ELSE 0 END) AS "February 2024",
    MAX(CASE WHEN month = '2024-01' THEN videocount ELSE 0 END) AS "January 2024",
    MAX(CASE WHEN month = '2023-12' THEN videocount ELSE 0 END) AS "December 2023",
    MAX(CASE WHEN month = '2023-11' THEN videocount ELSE 0 END) AS "November 2023",
    MAX(CASE WHEN month = '2023-10' THEN videocount ELSE 0 END) AS "October 2023",
    MAX(CASE WHEN month = '2023-09' THEN videocount ELSE 0 END) AS "September 2023",
    MAX(CASE WHEN month = '2023-08' THEN videocount ELSE 0 END) AS "August 2023"
FROM 
    video_month_counts
GROUP BY 
    channelname
ORDER BY 
    channelname;	





6) Monthly Video Counts and Percentage Change by Channel (Last 12 Months)	?

     WITH MonthlyVideoCounts AS (
    SELECT
        vd.ChannelID,
        DATE_TRUNC('month', vd.PublishDate) AS Month,
        COUNT(*) AS VideoCount
    FROM
        video_details vd
    GROUP BY
        vd.ChannelID,
        DATE_TRUNC('month', vd.PublishDate)
),
RankedData AS (
    SELECT
        mv.ChannelID,
        mv.Month,
        mv.VideoCount,
        LAG(mv.VideoCount) OVER (PARTITION BY mv.ChannelID ORDER BY mv.Month) AS PreviousMonthCount
    FROM
        MonthlyVideoCounts mv
),
ChannelData AS (
    SELECT
        cd.ChannelID,
        cd.ChannelName
    FROM
        channel_details cd
)
SELECT
    cd.ChannelName,
    rd.Month,
    rd.VideoCount,
    CASE
        WHEN rd.PreviousMonthCount IS NULL THEN NULL
        WHEN rd.PreviousMonthCount = 0 THEN NULL
        ELSE
            (rd.VideoCount - rd.PreviousMonthCount) * 100.0 / rd.PreviousMonthCount
    END AS PercentChange
FROM
    RankedData rd
    JOIN ChannelData cd ON rd.ChannelID = cd.ChannelID
ORDER BY
    cd.ChannelName,
    rd.Month;					
	
	
    7) Channels with Consistent Monthly Activity Over the Past 12 Months ?

        WITH MonthlyCounts AS (
    SELECT
        ChannelID,
        DATE_TRUNC('month', PublishDate) AS Month,
        COUNT(*) AS VideoCount
    FROM
        video_details
    WHERE
        PublishDate >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '12 months'
        AND PublishDate < DATE_TRUNC('month', CURRENT_DATE) -- Exclude the current month
    GROUP BY
        ChannelID,
        DATE_TRUNC('month', PublishDate)
),
ConsistentChannels AS (
    SELECT
        ChannelID,
        COUNT(*) AS ActiveMonths
    FROM
        MonthlyCounts
    WHERE
        VideoCount > 0
    GROUP BY
        ChannelID
    HAVING
        COUNT(DISTINCT Month) = 12 
)
SELECT
    cd.ChannelName
FROM
    ConsistentChannels cc
JOIN
    channel_details cd ON cc.ChannelID = cd.ChannelID
ORDER BY
    cd.ChannelName;					
					
	

8) Outliers : Top-Performing Videos: Exceptional View Counts by Channel (Last 12 Months)?
    
  SELECT  distinct c.channelname, v.title, v.viewcount
FROM channel_details c
JOIN video_details v on v.channelid = c.channelid
WHERE v.viewcount > (SELECT AVG(viewcount) + 2 * STDDEV(viewcount) FROM video_details)
ORDER BY v.viewcount DESC;				
					
