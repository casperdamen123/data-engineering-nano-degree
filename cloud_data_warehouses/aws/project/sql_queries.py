import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
# Raw staging tables

staging_events_table_create= """
                             CREATE TABLE IF NOT EXISTS staging_events (
                                 artist TEXT,
                                 auth TEXT,
                                 firstName TEXT,
                                 gender TEXT,
                                 itemInSession INT,
                                 lastName TEXT,
                                 length FLOAT,
                                 level TEXT,
                                 location TEXT,
                                 method TEXT,
                                 page TEXT,
                                 registration BIGINT,
                                 sessionId INT,
                                 song TEXT,
                                 status INT,
                                 ts text,
                                 userAgent TEXT,
                                 userId INT
                             );
                             """

staging_songs_table_create = """
                             CREATE TABLE IF NOT EXISTS staging_songs (
                                 artist_id TEXT,
                                 artist_latitude FLOAT,
                                 artist_longitude FLOAT,
                                 artist_location TEXT,
                                 artist_name TEXT,
                                 song_id TEXT,
                                 title TEXT,
                                 duration FLOAT,
                                 num_songs INT,
                                 year INT
                             );
                             """

# Business tables

songplay_table_create = """
                        CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id INT IDENTITY(0,1),
                            start_time TIMESTAMP NOT NULL REFERENCES time(start_time) SORTKEY,
                            user_id TEXT NOT NULL REFERENCES users(user_id) DISTKEY,
                            level TEXT, 
                            song_id TEXT NOT NULL REFERENCES songs(song_id), 
                            artist_id TEXT NOT NULL REFERENCES artists(artist_id), 
                            session_id TEXT NOT NULL, 
                            location TEXT, 
                            user_agent TEXT,
                            PRIMARY KEY (songplay_id)
                        );
                        """

user_table_create = """
                    CREATE TABLE IF NOT EXISTS users (
                        user_id TEXT DISTKEY, 
                        first_name TEXT, 
                        last_name TEXT, 
                        gender TEXT, 
                        level TEXT,
                        PRIMARY KEY (user_id)
                    );
                    """

song_table_create = """
                    CREATE TABLE IF NOT EXISTS songs (
                        song_id TEXT DISTKEY, 
                        title TEXT, 
                        artist_id TEXT NOT NULL REFERENCES artists(artist_id), 
                        year INT NOT NULL SORTKEY, 
                        duration FLOAT NOT NULL,
                        PRIMARY KEY (song_id)
                    );
                    """

artist_table_create = """
                      CREATE TABLE IF NOT EXISTS artists (
                          artist_id TEXT DISTKEY, 
                          name TEXT NOT NULL, 
                          location TEXT, 
                          latitude FLOAT, 
                          longitude FLOAT,
                          PRIMARY KEY (artist_id)
                      );
                      """

time_table_create = """
                    CREATE TABLE IF NOT EXISTS time (
                        start_time TIMESTAMP,
                        hour INT NOT NULL, 
                        day INT NOT NULL, 
                        week INT NOT NULL, 
                        month INT NOT NULL, 
                        year INT NOT NULL, 
                        weekday INT NOT NULL,
                        PRIMARY KEY (start_time)
                    );
                    """

# INSERT DATA
# Staging tables

staging_events_copy = f"""
                       COPY staging_events FROM {config.get("S3", "LOG_DATA")}
                       CREDENTIALS {config.get("IAM_ROLE", "ARN")}
                       REGION 'us-west-2'
                       JSON {config.get("S3", "LOG_JSONPATH")};
                       """


staging_songs_copy = f"""
                      COPY staging_songs FROM {config.get("S3", "SONG_DATA")}
                      CREDENTIALS {config.get("IAM_ROLE", "ARN")}
                      REGION 'us-west-2'
                      JSON 'auto';
                      """


# Business tables

songplay_table_insert = f"""
                         INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                         SELECT DISTINCT
                            TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
                            userId AS user_id,
                            level,  
                            song_id,
                            artist_id,
                            sessionId AS session_id, 
                            location,
                            userAgent AS user_agent
                         FROM staging_songs ss
                         JOIN staging_events se
                         ON ss.title = se.song 
                            AND se.artist = ss.artist_name
                         WHERE page = 'NextSong';
                         """

user_table_insert = f"""
                     INSERT INTO users 
                     SELECT DISTINCT
                        userId AS user_id,
                        firstName AS first_name,
                        lastName AS last_name,
                        gender AS gender,
                        level AS level
                     FROM staging_events
                     WHERE userId IS NOT NULL
                        AND page = 'NextSong';
                     """

song_table_insert = f"""
                     INSERT INTO songs 
                     SELECT DISTINCT
                        song_id,
                        title,
                        artist_id,
                        year,
                        duration
                     FROM staging_songs
                     WHERE song_id IS NOT NULL;
                     """

artist_table_insert = f"""
                      INSERT INTO artists 
                      SELECT DISTINCT
                         artist_id,
                         artist_name,
                         artist_location,
                         artist_latitude,
                         artist_longitude
                      FROM staging_songs
                      WHERE artist_id IS NOT NULL;
                      """

time_table_insert = f"""
                     INSERT INTO time 
                     SELECT
                        TIMESTAMP 'epoch' + (ts / 1000) * INTERVAL '1 second' AS start_time,
                        EXTRACT(hour FROM start_time) AS hour,
                        EXTRACT(day FROM start_time) AS day,
                        EXTRACT(week FROM start_time) AS week,
                        EXTRACT(weekday FROM start_time) AS weekday,
                        EXTRACT(month FROM start_time) AS month,
                        EXTRACT(year FROM start_time) AS year
                     FROM staging_events
                     WHERE page = 'NextSong';
                     """

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy] 
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]