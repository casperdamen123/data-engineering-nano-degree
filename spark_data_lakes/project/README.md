# Project overview
This code is used to setup the Sparkify Data Lake in S3.

The goal of this repository is to allow business to understand what songs users of the app are listening to. In total there are 5 tables available, 1 fact table and 4 dimension tables.

# Technical details

## 1. Data Lake structure 
\
__Fact Table__

Songplays
 - `start_time`
 - `month`
 - `year`
 - `user_id`
 - `level`
 - `song_id`
 - `artist_id`
 - `session_id`
 - `location`
 - `user_agent`
 
__Dimension Tables__

Users
 - `user_id`
 - `first_name`
 - `last_name`
 - `gender`
 - `level` 

Songs
 - `song_id`
 - `title`
 - `artist_id`
 - `year`
 - `duration`

Artists
 - `artist_id`
 - `name`
 - `location`
 - `latitude`
 - `longitude`

Time
 - `start_time)`
 - `hour`
 - `day`
 - `week`
 - `month`
 - `year`
 - `weekday`

## 2. Setting up Data Lake
Before running the etl.py script to setup the Data Lake in S3, it's important to configure the `dwh_example.cfg` configuration file. Explanation of the parameters to insert can be found in the file itself. After completion, it's important to change the file name to `aws.cfg`.

Next, the following files can be used to setup the Data Lake:

- `etl.py`: run to process the raw data files and push to the datalake on S3

Please use `python <file_name>.py` to run the files mentioned above.