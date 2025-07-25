DROP TABLE IF EXISTS METsByMinute;
CREATE TABLE METsByMinute (
user_id VARCHAR (50),
ActivityMinute TIMESTAMP,
METs NUMERIC(10)
);

COPY METsByMinute FROM '/Users/reedw.solomon/Data_Folder/minuteMETsNarrow_merged_3_4.csv' DELIMITER ',' CSV HEADER;
DELETE FROM METsByMinute WHERE activityminute::date = '2016-04-12';
COPY METsByMinute FROM '/Users/reedw.solomon/Data_Folder/minuteMETsNarrow_merged_4_5.csv' DELIMITER ',' CSV HEADER;

SELECT *	

FROM METsByMinute

LIMIT 100

SELECT
	MIN(activityminute) AS earliest_start,
	MAX(activityminute) AS latest_end

FROM METsByMinute


SELECT 
	user_id,
	--activityminute,
	ROUND(AVG(mets/10), 2) AS actual_mets_per_minute,
	--ROUND((1440 * AVG(mets/10)), 2) AS actual_mets_per_full_day,
	COUNT(*)/1440 AS num_full_days

FROM METsByMinute

GROUP BY user_id

--create calculated fields to determine faults in data. Compare number of minutes for each ID with number of unique timestamps. Establish time range.

SELECT
	user_id,
	--COUNT(*) AS num_rows,
	COUNT(DISTINCT activityminute::date) AS num_days,
	CEIL(EXTRACT (EPOCH FROM MAX(activityminute)-MIN(activityminute))/86400) AS day_range,
	CEIL(EXTRACT (EPOCH FROM MAX(activityminute)-MIN(activityminute))/86400) - COUNT(DISTINCT activityminute::date) AS days_missed,
	ROUND((COUNT(*)/CEIL(EXTRACT (EPOCH FROM MAX(activityminute)-MIN(activityminute))/86400))/14.40, 2) AS percent_minutes_tracked_per_day,
	MIN(activityminute) AS start_time,
	MAX(activityminute) AS end_time,
	ROUND((SUM(mets-10)/10)/COUNT(*), 2) AS avg_total_mets_above_rest,
	ROUND((SUM(mets-10)/10)/COUNT(*)*1440, 2) AS daily_total_mets_above_rest

FROM METsByMinute	

--WHERE activityminute :: date = '2016-03-12'

GROUP BY user_id

ORDER BY user_id


SELECT
	activityminute,
	activityminute :: date AS day
FROM METsByMinute

--Check end dates & times for first part of MET table (March to April)

DROP TABLE IF EXISTS METsByMinuteApriltoMaySection;
CREATE TABLE METsByMinuteApriltoMaySection (
user_id VARCHAR (50),
ActivityMinute TIMESTAMP,
METs NUMERIC(10)
);

COPY METsByMinuteApriltoMaySection FROM '/Users/reedw.solomon/Data_Folder/minuteMETsNarrow_merged_4_5.csv' DELIMITER ',' CSV HEADER;

SELECT 
	*
	

FROM METsByMinuteApriltoMaySection

ORDER BY activityminute ASC

LIMIT 10

--Transform table to track MET by hour rather than minute
CREATE TABLE METsCaloriesByHour AS
WITH METsByHour AS (
SELECT
	user_id,
	CONCAT(activity_hour, ':00:00') :: timestamp AS activity_hour,
	ROUND(SUM(mets)/10, 2) AS mets


FROM (
	SELECT
		user_id,
		activityminute :: VARCHAR(13) AS activity_hour,
		mets		
 	FROM
 METsByMinute
)

--ORDER BY activity_hour DESC
GROUP BY user_id, activity_hour
)


SELECT 
	t1.user_id,
	t1.activity_hour AS hour,
	ROUND(t1.mets, 1) AS mets,
	t2.calories

FROM
	METsByHour AS t1

INNER JOIN CaloriesByHour AS t2 ON t1.activity_hour = t2.hour AND t1.user_id = t2.user_id --tables are joined on both userID and hour columns
;

--Join usernames table to MET's/calories by hour table
SELECT
	t2.username,
	t1.hour,
	t1.calories,
	t1.mets,
	t3.avg_weight

FROM METsCaloriesByHour AS t1

INNER JOIN UserNamesBellaBeat AS t2 ON t1.user_id = t2.user_id

LEFT JOIN WeightBellaBeat AS t3 ON t1.user_id = t3.user_id

ORDER BY t2.username, t1.hour

--LIMIT 1000
	
--check for min and max of calorie/MET's ratio, grouped by user
SELECT
	t2.username,
	MIN(t1.mets/t1.calories) AS min_cal_met_ratio,
	MAX(t1.mets/t1.calories) AS max_cal_met_ratio,
	AVG(t1.mets/t1.calories) AS avg_cal_met_ratio,
	ROUND(MAX(t1.mets/t1.calories)-MIN(t1.mets/t1.calories), 2) AS ratio_variation	

FROM METsCaloriesByHour AS t1

INNER JOIN UserNamesBellaBeat AS t2 ON t1.user_id = t2.user_id

GROUP BY t2.username

ORDER BY t2.username


--Create table for calories by hour in PostgreSQL. To be joined with METs by hour table.
DROP TABLE IF EXISTS CaloriesByHour;
CREATE TABLE CaloriesByHour (
user_id VARCHAR(50),
hour TIMESTAMP,
calories NUMERIC(50)
);

COPY CaloriesByHour FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/Google-Data-Analytics-Case-Study-2-BellaBeat/postgres_exports/calories_by_hour.csv' DELIMITER ',' CSV HEADER;



SELECT *


FROM CaloriesByHour

LIMIT 100

--Create table to translate user IDs to a more readable username
DROP TABLE IF EXISTS UserNamesBellaBeat;
CREATE TABLE UserNamesBellaBeat (
user_id VARCHAR(50),
username VARCHAR(50)
);
COPY UserNamesBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/Google-Data-Analytics-Case-Study-2-BellaBeat/postgres_exports/bellabeat_usernames.csv' DELIMITER ',';
SELECT *
FROM UserNamesBellaBeat

--Create table for weight
DROP TABLE IF EXISTS WeightBellaBeat;
CREATE TABLE WeightBellaBeat (
user_id VARCHAR(50),
avg_weight DOUBLE PRECISION, 
min_weight DOUBLE PRECISION,
max_weight DOUBLE PRECISION,
weight_range DOUBLE PRECISION
);

COPY WeightBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/Google-Data-Analytics-Case-Study-2-BellaBeat/postgres_exports/weight_table.csv' DELIMITER ',' CSV HEADER;

SELECT *

FROM WeightBellaBeat


--Create table for sleep data using both monthly tables
DROP TABLE IF EXISTS SleepBellaBeat;
CREATE TABLE SleepBellaBeat (
user_id VARCHAR(50),
date TIMESTAMP,
value NUMERIC (50),
log_id VARCHAR(50)
);

COPY SleepBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/minuteSleep_merged.csv' DELIMITER ',' CSV HEADER;
COPY SleepBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/minuteSleep_merged2.csv' DELIMITER ',' CSV HEADER;


SELECT *

FROM SleepBellaBeat

LIMIT 100

--determine range of "value" field
SELECT
	user_id,
	MIN(value),
	MAX(value),
	AVG(value),
	SUM(CASE WHEN value=1 THEN 1 ELSE 0 END) AS _1_minutes,
	SUM(CASE WHEN value=2 THEN 1 ELSE 0 END) AS _2_minutes,
	SUM(CASE WHEN value=3 THEN 1 ELSE 0 END) AS _3_minutes,
	COUNT(*) AS total_minutes

FROM SleepBellaBeat

GROUP BY user_id

ORDER BY AVG(value)

--transform table to organize by hour
SELECT
	user_id,
	EXTRACT(HOUR FROM date)
	SUM(CASE WHEN value=1 THEN 1 ELSE 0 END) AS _1_minutes,
	SUM(CASE WHEN value=2 THEN 1 ELSE 0 END) AS _2_minutes,
	SUM(CASE WHEN value=3 THEN 1 ELSE 0 END) AS _3_minutes

	
