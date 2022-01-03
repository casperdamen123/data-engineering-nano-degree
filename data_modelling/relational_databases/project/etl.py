import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, song_file_path: str):
    
    """
    Args:
        cur (psycopg2.conn.cursor): Cursor connected to database to execute the queries
        file_path (str): Song file location
    Returns:
        None: Pushes the song and artist record to the PostgreSQL database
    """

    # Open song file and save as df
    song_df = pd.read_json(path_or_buf=song_file_path, lines=True)
    
    # Insert song record
    song_data = song_df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    try:
        cur.execute(song_table_insert, song_data)
    except psycopg2.Error as e:
        print(e)
    
    # Insert artist record
    artist_data = song_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0].tolist()
    try:
        cur.execute(artist_table_insert, artist_data)
    except psycopg2.Error as e:
        print(e)


def process_log_file(cur, log_file_path: str):

    """
    Args:
        cur (psycopg2.conn.cursor): Cursor connected to database to execute the queries
        log_file_path (str): Log file location
    Returns:
        None: Pushes the time, user and songplay record to the PostgreSQL database
    """

    # Open log file and save as df
    log_df = pd.read_json(path_or_buf=log_file_path, lines=True)

    # Filter by NextSong action
    song_log_df = log_df[log_df['page'] == 'NextSong'] 

    # Convert timestamp column to datetime
    song_log_time = pd.to_datetime(song_log_df['ts']) 
    
    # Insert time data records
    time_df =  pd.DataFrame(
            dict(
                start_time=song_log_time,
                hour=song_log_time.dt.hour,
                day=song_log_time.dt.day,
                week=song_log_time.dt.isocalendar().week,
                weekday=song_log_time.dt.weekday,
                month=song_log_time.dt.month,
                year=song_log_time.dt.year,
            )
    )

    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, row)
        except psycopg2.Error as e:
            print(e)

    # Load user table
    user_df = song_log_df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # Insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print(e)

    # Load songplay table
    songplay_df_raw = song_log_df[['ts', 'userId', 'level', 'location', 'sessionId', 'userAgent', 'song', 'artist', 'length']]
    songplay_df = songplay_df_raw.assign(ts=pd.to_datetime(songplay_df_raw['ts'])) 
        
    # Insert songplay records 
    for index, row in songplay_df.reset_index().iterrows():
        
        # Add songId and artistId from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e:
            print(e)

        if results:
            songId, artistId = results
        else:
            songId, artistId = None, None

        songplay_data = (index, row.ts, row.userId, row.level, songId, artistId, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print(e)


def process_data(cur, conn, file_path, func):
    
    """
    Args:
        cur (psycopg2.conn.cursor): Cursor connected to database to execute the queries
        conn (psycopg2.conn): Connection to PostgreSQL database
        file_path (str): Main folder location containing all JSON data files
        func (func): Function to be executed to process 
    Returns:
        None: Prints the number of processed files
    """
        
    # Get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(file_path):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, file_path))

    # Iterate over files and process
    for i, data_file in enumerate(all_files, 1):
        func(cur, data_file)
        try:
            conn.commit()
        except psycopg2.Error as e:
            print(e)
        print('{}/{} files processed.'.format(i, num_files))


def main():
    try: 
        conn = psycopg2.connect("host=localhost port=5432 dbname=sparkifydb user=postgres password=feyenoord")
        cur = conn.cursor()
    except psycopg2.Error as e:
        print(e)

    process_data(cur, conn, file_path='data/song_data', func=process_song_file)
    process_data(cur, conn, file_path='data/log_data', func=process_log_file)

    conn.close()

if __name__ == "__main__":
    main()