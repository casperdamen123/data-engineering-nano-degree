# Project overview
This aws Redshift database has been setup in order to analyze the collected data on songs and user activity on the new music streaming app from Sparkify. 

The goal of this database is to allow business to understand what songs users of the app are listening to. In total there are 5 tables available, 1 fact table and 4 dimension tables.

# Technical details

## 1. Database structure 
\
__Fact Table__

Songplays
 - `songplay_id (serial)`
 - `start_time (time)`
 - `user_id (text)`
 - `level (text)`
 - `song_id (text)`
 - `artist_id (text)`
 - `session_id (integer)`
 - `location (text)`
 - `user_agent (text)`
 
__Dimension Tables__

Users
 - `user_id (text)`
 - `first_name (text)`
 - `last_name (text)`
 - `gender (text)`
 - `level(text)` 

Songs
 - `song_id (text)`
 - `title (text)`
 - `artist_id (text)`
 - `year (integer)`
 - `duration(float)`

Artists
 - `artist_id (text)`
 - `name (text)`
 - `location (text)`
 - `latitude (float)`
 - `longitude (float)`

Time
 - `start_time (timestamp)`
 - `hour (integer)`
 - `day (integer)`
 - `week (integer)`
 - `month (integer)`
 - `year (integer)`
 - `weekdag (integer)`
## 2. Setting up database
Before running the scripts to setup the database, it's important to configure your customize the `dwh_example.cfg` configuration file. Explanation of the parameters to insert can be found in the file itself. After completion, it's important to change the file name to `dwh.cfg`.

Next, the following files can be used to setup the database:

- `create_tables.py`: run to create the the different tables
- `etl.py`: run to process the raw data files and push to the database

Please use `python <file_name>.py` to run the files mentioned above.

### 3. Additional file details
- `sql_queries.py`: collection of queries to create, insert and select tables