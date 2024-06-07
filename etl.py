import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime


def process_song_file(cur, filepath):

    """ The process_song_file procedure processes a song file. The filepath has been provided as an arugment.

    The song information is extracted in order to be stored in the songs dimension table, as well as, the
    artist information is extracted in order to be stored in the artists dimension table.

    INPUTS: 

    * cur - Cursor variable.
    * filepath - File path to the song file. """
    
    # open song file
    df = pd.read_json(filepath, lines = True)

    # insert song record
    song_data = df[['song_id', 
                    'title', 
                    'artist_id', 
                    'year', 
                    'duration']].copy()
    
    song_data = song_data.values[0].copy()    
    cur.execute(song_table_insert, song_data)
    
    # insert artist record  
    artist_data = df[['artist_id', 
                      'artist_name', 
                      'artist_location', 
                      'artist_latitude', 
                      'artist_longitude']].copy()
    
    artist_data = artist_data.values[0].copy()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):

    """ The process_log_file procedure processes log files. The filepath has been provided as an arugment.

    The time information is extracted in order to be stored in the time dimension table, as well as, the
    user information is extracted in order to be stored in the user dimension table.

    The fact table is created by iterating throught he records.   


    INPUTS: 

    * cur - Cursor variable.
    * filepath - File path to the log file. """
    
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime 
    df['ts'] = pd.to_datetime(df['ts'], unit = 'ms')
    
    # insert time data records

    time_data = (df.ts, 
                 df.ts.dt.hour, 
                 df.ts.dt.day, 
                 df.ts.dt.week, 
                 df.ts.dt.month, 
                 df.ts.dt.year, 
                 df.ts.dt.weekday)
    
    column_labels = ('start_time', 
                     'hour', 
                     'day', 
                     'week', 
                     'month', 
                     'year', 
                     'weekday')
    
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 
                  'firstName',
                  'lastName',
                  'gender',
                  'level']].copy()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

        
    
    
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            song_id, artist_id = results
            
        else:
            song_id, artist_id = None, None    
                              
        # insert songplay record
        songplay_data = (row.ts, 
                         row.userId, 
                         row.level, 
                         song_id, 
                         artist_id, 
                         row.sessionId, 
                         row.location, 
                         row.userAgent)
         
        cur.execute(songplay_table_insert, songplay_data)

def process_data(cur, conn, filepath, func):
    
    """ The process_data procedure processes all data files (JSON) in the directory. 
    
    The filepath has been provided as an arugment, as well as an argument for a function to call.

    JSON files are locatated and processes by the song_data or log_data functions when called.

    INPUTS: 

    * cur - Cursor variable.
    * filepath - File path to the data file. 
    * conn - Connection variable established with database.
    * func - Function to be executed when called."""
    
    # get all files matching extension from directory
    all_files = []
    
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        
        print('{}/{} files processed.'.format(i, num_files))

def main():
    conn = psycopg2.connect('host=127.0.0.1 dbname=sparkifydb user=student password=student')
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    
    conn.close()

if __name__ == '__main__':
    main()