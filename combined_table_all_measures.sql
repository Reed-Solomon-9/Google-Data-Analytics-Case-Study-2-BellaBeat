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
