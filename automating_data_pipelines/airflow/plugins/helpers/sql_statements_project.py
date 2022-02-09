class SqlQueries:

    # Creation of staging tables

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

    staging_events_table_create = """
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
    # Copying of staging tables

    copy_json_table = ("""
        COPY {table} 
        FROM '{s3_data_path}'
        ACCESS_KEY_ID '{aws_access_key}'
        SECRET_ACCESS_KEY '{aws_secret_key}'
        REGION '{region}'
        JSON '{json_path}';
    """)

    # Creation of business tables

    songplay_table_create = """
                            CREATE TABLE IF NOT EXISTS fct_songplays (
                                songplay_id TEXT,
                                start_time TIMESTAMP,
                                user_id TEXT,
                                level TEXT, 
                                song_id TEXT, 
                                artist_id TEXT, 
                                session_id TEXT, 
                                location TEXT, 
                                user_agent TEXT,
                                PRIMARY KEY (songplay_id)
                            );
                            """

    user_table_create = """
                        CREATE TABLE IF NOT EXISTS dim_users (
                            user_id TEXT, 
                            first_name TEXT, 
                            last_name TEXT, 
                            gender TEXT, 
                            level TEXT,
                            PRIMARY KEY (user_id)
                        );
                        """

    song_table_create = """
                        CREATE TABLE IF NOT EXISTS dim_songs (
                            song_id TEXT, 
                            title TEXT, 
                            artist_id TEXT, 
                            year INT, 
                            duration FLOAT,
                            PRIMARY KEY (song_id)
                        );
                        """

    artist_table_create = """
                          CREATE TABLE IF NOT EXISTS dim_artists (
                              artist_id TEXT, 
                              name TEXT, 
                              location TEXT, 
                              latitude FLOAT, 
                              longitude FLOAT,
                              PRIMARY KEY (artist_id)
                          );
                          """

    time_table_create = """
                        CREATE TABLE IF NOT EXISTS dim_time (
                            start_time TIMESTAMP,
                            hour INT, 
                            day INT, 
                            week INT, 
                            month INT, 
                            year INT, 
                            weekday INT,
                            PRIMARY KEY (start_time)
                        );
                        """

    # Data insertion on business tables

    songplay_table_insert = ("""
        SELECT
            md5(events.sessionid || events.start_time) songplay_id,
            events.start_time, 
            events.userid, 
            events.level, 
            songs.song_id, 
            songs.artist_id, 
            events.sessionid, 
            events.location, 
            events.useragent
        FROM 
            (
             SELECT 
                *,
                TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
             FROM staging_events
             WHERE page='NextSong'
             ) events
        LEFT JOIN staging_songs songs
        ON 
            events.song = songs.title
            AND events.artist = songs.artist_name
            AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT DISTINCT 
            userid, 
            firstname, 
            lastname, 
            gender, 
            level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT DISTINCT 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT DISTINCT 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT 
            start_time, 
            EXTRACT(hour FROM start_time), 
            EXTRACT(day FROM start_time), 
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time), 
            EXTRACT(year FROM start_time), 
            EXTRACT(dayofweek FROM start_time)
        FROM fct_songplays
    """)
