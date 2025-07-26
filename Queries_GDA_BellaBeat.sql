--Create table for MET's by minute in PostgreSQL
DROP TABLE IF EXISTS METsByMinute;
CREATE TABLE METsByMinute (
user_id VARCHAR (50),
ActivityMinute TIMESTAMP,
METs NUMERIC(10)
);

COPY METsByMinute FROM '/Users/reedw.solomon/Data_Folder/minuteMETsNarrow_merged_3_4.csv' DELIMITER ',' CSV HEADER;
DELETE FROM METsByMinute WHERE activityminute::date = '2016-04-12';
COPY METsByMinute FROM '/Users/reedw.solomon/Data_Folder/minuteMETsNarrow_merged_4_5.csv' DELIMITER ',' CSV HEADER;

--check result
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

LEFT JOIN WeightBellaBeat AS t3 ON t1.user_id = t3.user_id, 

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


--Check result
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
--Check result
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
--Check result
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

--Check result
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

CREATE TABLE SleepBellaBeatByHour AS (
SELECT
	user_id,
	CONCAT(hour, ':00:00'):: TIMESTAMP AS hour,
	SUM(CASE WHEN value=1 THEN 1 ELSE 0 END) AS _1_minutes,
	SUM(CASE WHEN value=2 THEN 1 ELSE 0 END) AS _2_minutes,
	SUM(CASE WHEN value=3 THEN 1 ELSE 0 END) AS _3_minutes,
	COUNT(*) AS total_minutes,
	ROUND(AVG(value), 2) AS avg_value,
	SUM(value) AS total_value

FROM (
	SELECT
		user_id,
		date :: VARCHAR(13) AS hour,
		value,
		log_id
	FROM SleepBellaBeat	
	) 

GROUP BY user_id, hour
)


--Join sleep table to METs, Calories, Weight
SELECT
	t2.username,
	t1.hour,
	t1.calories,
	t1.mets,
	t3.avg_weight,
	t4._1_minutes AS sleep_1_mins,
	t4._2_minutes AS sleep_2_mins,
	t4._3_minutes AS sleep_3_mins,
	t4.total_minutes AS total_sleep_mins,
	t4.avg_value AS avg_sleep_value,
	t4.total_value AS total_sleep_value
	

FROM METsCaloriesByHour AS t1

INNER JOIN UserNamesBellaBeat AS t2 ON t1.user_id = t2.user_id

LEFT JOIN WeightBellaBeat AS t3 ON t1.user_id = t3.user_id 
LEFT JOIN SleepBellaBeatByHour AS t4 ON t4.user_id = t1.user_id AND t4.hour = t1.hour

ORDER BY t2.username, t1.hour

--create table for hourly intensities **this was later replaced with a better hourly table transformed from the intensity-by-minute table
DROP TABLE IF EXISTS IntensityByHourBellaBeat;
CREATE TABLE IntensityByHourBellaBeat (
user_id VARCHAR(50),
hour TIMESTAMP,
total_intensity NUMERIC(50),
avg_intensity NUMERIC(50)
);

COPY IntensityByHourBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/hour_intensities_march_may.csv' DELIMITER ',' CSV HEADER;

--check result
SELECT *

FROM IntensityByHourBellaBeat

ORDER BY hour DESC
--LIMIT 100

--create table for *minute* intensities to add counts of intensity levels to hourly table
DROP TABLE IF EXISTS IntensityByMinuteBellaBeat3;
CREATE TABLE IntensityByMinuteBellaBeat3 (
user_id VARCHAR(50),
hour TIMESTAMP,
intensity NUMERIC(50)
);

COPY IntensityByMinuteBellaBeat3 FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/minuteIntensitiesNarrow_merged_3_4.csv' DELIMITER ',' CSV HEADER;

--check result (1,445,040 rows)
SELECT *,
	(
	SELECT 
		COUNT(*) 
	FROM IntensityByMinuteBellaBeat3
	)


FROM IntensityByMinuteBellaBeat3;




--remove duplicate entries (everything from April 12th in the March-April table is duplicated in the April-May table)
DROP TABLE IF EXISTS IntensityByMinuteBellaBeat;
CREATE TABLE IntensityByMinuteBellaBeat AS (
SELECT *
	--,
	--hour::VARCHAR(10) AS date_string

FROM IntensityByMinuteBellaBeat3

WHERE hour::VARCHAR(10) <> '2016-04-12'

--ORDER BY hour DESC
);

SELECT *,
	(
	SELECT 
		COUNT(*)
	FROM IntensityByMinuteBellaBeat
	)
FROM IntensityByMinuteBellaBeat

ORDER BY hour DESC

--add rows from April to May
COPY IntensityByMinuteBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/minuteIntensitiesNarrow_merged_4_5.csv' DELIMITER ',' CSV HEADER;

--check result
SELECT COUNT(*)

FROM IntensityByMinuteBellaBeat
--check again
SELECT 
	user_id,
	AVG(intensity)

FROM IntensityByMinuteBellaBeat

GROUP BY user_id

--transform into hourly table that includes counts of intensities (replace old hourly table)
DROP TABLE IF EXISTS IntensityByHourBellaBeatNew;
CREATE TABLE IntensityByHourBellaBeatNew AS 
SELECT
	user_id,
	CONCAT(hour, ':00:00'):: TIMESTAMP AS hour,
	SUM(CASE WHEN intensity=1 THEN 1 ELSE 0 END) AS light_minutes,
	SUM(CASE WHEN intensity=2 THEN 1 ELSE 0 END) AS moderate_minutes,
	SUM(CASE WHEN intensity=3 THEN 1 ELSE 0 END) AS intense_minutes,
	SUM(CASE WHEN intensity=0 THEN 1 ELSE 0 END) AS sedentary_minutes,
	COUNT(*) AS total_minutes,
	ROUND(AVG(intensity), 2) AS avg_intensity,
	SUM(intensity) AS total_intensity

FROM (
	SELECT
		user_id,
		hour :: VARCHAR(13) AS hour,
		intensity
	FROM IntensityByMinuteBellaBeat	
	) 

GROUP BY 
	user_id,
	hour
	
ORDER BY hour, user_id;

SELECT*
FROM IntensityByHourBellaBeatNew
LIMIT 100

--compare new hourly table to old hourly table to ensure the new one makes sense and check for discrepancies
WITH AggNewHourlyTable AS (
SELECT 
	user_id,
	COUNT(*) AS total_minutes,
	AVG(total_intensity) AS avg_intensity_hour

FROM IntensityByHourBellaBeatNew

GROUP BY user_id
)
SELECT
	t1.user_id,
	t1.total_minutes,
	t2.old_total_minutes,
	t1.avg_intensity_hour,
	t2.old_avg_intensity_hour

FROM AggNewHourlyTable AS t1

FULL OUTER JOIN (
	SELECT	
		user_id,
		COUNT(*) AS old_total_minutes,
		AVG(total_intensity) AS old_avg_intensity_hour

	FROM IntensityByHourBellaBeat	

	GROUP BY user_id
) AS t2 ON t1.user_id = t2.user_id

--full new hourly table to compare with old hourly table in Sheets **There were 6 rows added to then end of the hourly table from the original dataset that were absent from the per-minute table. I moved on without them.
SELECT
	user_id,
	hour,
	total_intensity,
	avg_intensity

FROM IntensityByHourBellaBeatNew


--create table for hourly steps

DROP TABLE IF EXISTS StepsByHourBellaBeat;
CREATE TABLE StepsByHourBellaBeat (
user_id VARCHAR(50),
hour TIMESTAMP,
steps NUMERIC(50)
);

COPY StepsByHourBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/hourlySteps_merged_march_may.csv' DELIMITER ',' CSV HEADER;

--check result
SELECT *
FROM StepsByHourBellaBeat
LIMIT 100


--join steps and intensity tables to larger table

WITH AggSummaryTable AS (
SELECT
	t2.username,
	t1.user_id,
	t1.hour,
	t1.calories,
	t1.mets,
	t3.avg_weight,
	t4._1_minutes AS sleep_1_mins,
	t4._2_minutes AS sleep_2_mins,
	t4._3_minutes AS sleep_3_mins,
	t4.total_minutes AS total_sleep_mins,
	t4.avg_value AS avg_sleep_value,
	t4.total_value AS total_sleep_value,
	t5.intense_minutes AS intense_mins,
	t5.moderate_minutes AS moderate_mins,
	t5.light_minutes AS light_mins,
	t5.sedentary_minutes AS sedentary_mins,
	t5.total_intensity AS total_intensity_hour,
	t5.avg_intensity AS avg_intensity_per_min,
	t6.steps
	

FROM METsCaloriesByHour AS t1

INNER JOIN UserNamesBellaBeat AS t2 ON t1.user_id = t2.user_id

LEFT JOIN WeightBellaBeat AS t3 ON t1.user_id = t3.user_id 
LEFT JOIN SleepBellaBeatByHour AS t4 ON t4.user_id = t1.user_id AND t4.hour = t1.hour
LEFT JOIN IntensityByHourBellaBeatNew AS t5 ON t5.user_id = t1.user_id AND t5.hour = t1.hour
LEFT JOIN StepsByHourBellaBeat AS t6 ON t6.user_id = t1.user_id AND t6.hour = t1.hour


ORDER BY t2.username, t1.hour
)

--determine who measured sleep and their total sleep minutes

SELECT 
	user_id,
	SUM(total_sleep_value)

FROM AggSummaryTable

GROUP BY user_id

HAVING SUM(total_sleep_value) IS NOT NULL

ORDER BY user_id

--determine who measured intensity and their average hourly intensities

SELECT 
	user_id,
	AVG(total_intensity_hour)

FROM AggSummaryTable

GROUP BY user_id

HAVING SUM(total_intensity_hour) IS NOT NULL

ORDER BY user_id

--create tables for heartrate
DROP TABLE IF EXISTS HeartrateBySecondBellaBeat;
CREATE TABLE HeartrateBySecondBellaBeat (
user_id VARCHAR(50),
time TIMESTAMP,
heartrate NUMERIC(50)
);

COPY HeartrateBySecondBellaBeat FROM '/Users/reedw.solomon/Data_Folder/Google BellaBeat analysis/heartrate_seconds_3_4.csv' DELIMITER ',' CSV HEADER;
--check
SELECT *
FROM HeartrateBySecondBellaBeat
LIMIT 100

--explore heartrate data. create columns to better organize before transforming into an hourly table.
WITH MaxIncluded AS (

SELECT 
	t1.user_id,
	t1.time,
	t1.heartrate,
	t2.max_heartrate

FROM HeartrateBySecondBellaBeat AS t1

INNER JOIN (
	
	SELECT
		user_id,
		MAX(heartrate) AS max_heartrate
		
	FROM HeartrateBySecondBellaBeat
	
	GROUP BY user_id
	) AS t2 ON t1.user_id = t2.user_id
ORDER BY t1.time	
)

SELECT
	user_id,
	AVG(heartrate) AS avg_heartrate,
	COUNT(*) AS num_seconds_recorded,
	COUNT(*)/3600 AS num_hours_recorded,
	MIN(heartrate) AS min_heartrate,
	PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY heartrate) AS decile1_heartrate,
	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY heartrate) AS median_heartrate,
	PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY heartrate) AS decile9_heartrate,
	MAX(max_heartrate) AS max_heartrate,
	SUM(CASE WHEN heartrate > (max_heartrate/2) THEN 1 ELSE 0 END) AS num_effort_seconds
			
FROM MaxIncluded

GROUP BY user_id

--transform heartrate-by-seconds table into hourly heartrate table
DROP TABLE IF EXISTS HeartrateByHourBellaBeat;
CREATE TABLE HeartrateByHourBellaBeat AS
WITH MaxIncluded AS (

SELECT 
	t1.user_id,
	t1.time::VARCHAR(13) AS hour,
	t1.heartrate,
	t2.max_heartrate

FROM HeartrateBySecondBellaBeat AS t1

INNER JOIN (
	
	SELECT
		user_id,
		MAX(heartrate) AS max_heartrate
		
	FROM HeartrateBySecondBellaBeat
	
	GROUP BY user_id
	) AS t2 ON t1.user_id = t2.user_id
ORDER BY t1.time	
)

SELECT
	user_id,
	CONCAT(hour, ':00:00')::TIMESTAMP AS hour,
	COUNT(*) AS num_seconds_recorded,
	AVG(heartrate) AS avg_heartrate,
	MIN(heartrate) AS min_heartrate,
	PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY heartrate) AS decile1_heartrate,
	PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY heartrate) AS median_heartrate,
	PERCENTILE_DISC(0.9) WITHIN GROUP (ORDER BY heartrate) AS decile9_heartrate,
	MAX(max_heartrate) AS max_heartrate,
	SUM(CASE WHEN heartrate > (max_heartrate/2) THEN 1 ELSE 0 END) AS num_effort_seconds
			
FROM MaxIncluded

GROUP BY hour, user_id

ORDER BY user_id;

SELECT *

FROM HeartrateByHourBellaBeat

--**Query for combined table**

SELECT
	t2.username,
	t1.user_id,
	t1.hour,
	t1.calories,
	t1.mets,
	t3.avg_weight,
	t4._1_minutes AS sleep_1_mins,
	t4._2_minutes AS sleep_2_mins,
	t4._3_minutes AS sleep_3_mins,
	t4.total_minutes AS total_sleep_mins,
	t4.avg_value AS avg_sleep_value,
	t4.total_value AS total_sleep_value,
	t5.intense_minutes AS intense_mins,
	t5.moderate_minutes AS moderate_mins,
	t5.light_minutes AS light_mins,
	t5.sedentary_minutes AS sedentary_mins,
	t5.total_intensity AS total_intensity_hour,
	t5.avg_intensity AS avg_intensity_per_min,
	t6.steps,
	t7.avg_heartrate,
	t7.min_heartrate,
	t7.decile1_heartrate,
	t7.median_heartrate,
	t7.decile9_heartrate,
	t7.max_heartrate,
	t7.num_seconds_recorded AS heartrate_secs_tracked,
	t7.num_effort_seconds
	

FROM METsCaloriesByHour AS t1

INNER JOIN UserNamesBellaBeat AS t2 		ON t1.user_id = t2.user_id

LEFT JOIN WeightBellaBeat AS t3 			ON t1.user_id = t3.user_id 
LEFT JOIN SleepBellaBeatByHour AS t4 		ON t4.user_id = t1.user_id AND t4.hour = t1.hour
LEFT JOIN IntensityByHourBellaBeatNew AS t5 ON t5.user_id = t1.user_id AND t5.hour = t1.hour
LEFT JOIN StepsByHourBellaBeat AS t6 		ON t6.user_id = t1.user_id AND t6.hour = t1.hour
LEFT JOIN HeartrateByHourBellaBeat AS t7 	ON t7.user_id = t1.user_id AND t7.hour = t1.hour


ORDER BY t2.username, t1.hour
