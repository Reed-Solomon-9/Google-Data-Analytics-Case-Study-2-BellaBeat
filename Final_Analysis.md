Google Data Analytics Case Study: BellaBeat Fitness Trackers

Project Overview

This analysis focuses on user behavior for 35 users who used fitness trackers during the period from March 12, 2016 to May 12, 2016. Users tracked various metrics, and some metrics were only recorded by a subset of the total user population. Metrics tracked included steps, calories, sleep, heartrate, and weight.
The goal for this project is to explore user behavior with fitness tracking devices.

Preparation, Data Integrity, ETL, and Transformations

The dataset has 29 tables, organized by time units: day, hour, minute, second. My analysis combines the metrics by user and in time to determine relationships between the measurements and what users actually tracked. 
There are 35 unique entries for “user_id” in total among these tables. In the tables for exercise intensity, calories, METs, and steps, all 35 user IDs are present. Only subsets of users measured weight, sleep, and heart rate. 4 of the 35 users measured every metric that was in the dataset.



I converted the time intervals to hours and stitched all the tables together to compare the measurements side-by-side. I aggregated the tables that were organized by seconds and minutes, and used UNION and JOIN statements. There was some overlap between the March-April and April-May time series, but the overlap was with identical rows and the duplicates were easily removed.

I loaded a complete table into Tableau featuring the 35 user ID’s and the total date range (March 12, 2016 at 12:00 AM through May 12, 2016 at 3 PM). This table allows comparisons to be made between all the measurements present in the dataset. With the table loaded into Tableau, it was fairly frictionless to use table calculations for any data transformation that was needed after this point. I calculated percentages of metrics like exercise intensity minutes and sleep value. I also created simple usernames like “User 01” to improve readability. 

Analysis

The first thing I noticed about the data was that there were four metrics that had all 35 users present: steps, MET’s, calories, and exercise intensity. My suspicion was that these were all derived from the same measurements, perhaps a step counter. In particular, there isn’t really a way to directly measure “calories”, whether that means calories consumed or burned. I charted calories and MET’s together and the resulting relationship was close to linear, and it was basically perfectly linear when broken down by user.

