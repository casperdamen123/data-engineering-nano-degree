## Project overview
This database has been setup in order to analyze the collected data on songs and user activity on the new music streaming app from Sparkify. 

The goal of this database is to allow business to understand what songs users of the app are listening to.

## Technical details

### 1. Database structure 
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

### 2. Setting up database
The following files can be used to setup the database:

- `create_tables.py`: run to create the the different tables
- `etl.py`: run to process the raw data files and push to the database

Please use `python <file_name>.py` to run the files mentioned above.

### 3. Additional file details
- `data/log_data`: contains the raw user log files in json format for 2018-11
- `data/song_data`: contains the raw song files in json format, categorized by the first three letters of the song id
- `etl.ipynb`: used to develop and test extract transform and load process
- `sql_queries.py`: collection of queries to create, insert and select tables
- `test.ipynb`: used to preview generated tables