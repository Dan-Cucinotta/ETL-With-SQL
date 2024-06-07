# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = """CREATE TABLE IF NOT EXISTS songplays (songplay_id SERIAL PRIMARY KEY NOT NULL, 
                                                                 start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
                                                                 user_id INT NOT NULL, 
                                                                 level TEXT, 
                                                                 song_id VARCHAR(25), 
                                                                 artist_id VARCHAR(25), 
                                                                 session_id TEXT, 
                                                                 location TEXT, 
                                                                 user_agent TEXT)"""

user_table_create = """CREATE TABLE IF NOT EXISTS users (user_id INT PRIMARY KEY NOT NULL, 
                                                         first_name TEXT NOT NULL, 
                                                         last_name TEXT NOT NULL, 
                                                         gender TEXT, 
                                                         level TEXT NOT NULL)"""

song_table_create = """CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR(25) PRIMARY KEY NOT NULL, 
                                                         title TEXT, 
                                                         artist_id VARCHAR(25), 
                                                         year INT, 
                                                         duration FLOAT)"""

artist_table_create = """CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR(25) PRIMARY KEY NOT NULL, 
                                                             artist_name TEXT, 
                                                             artist_location TEXT, 
                                                             artist_latitude FLOAT, 
                                                             artist_longitude FLOAT)"""

time_table_create = """CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP WITHOUT TIME ZONE PRIMARY KEY NOT NULL, 
                                                             hour INT, 
                                                             day INT, 
                                                             week INT, 
                                                             month INT, 
                                                             year INT, 
                                                             weekday INT)"""

# Insert Records

songplay_table_insert = """INSERT INTO songplays (start_time, 
                                                  user_id, 
                                                  level, 
                                                  song_id, 
                                                  artist_id, 
                                                  session_id, 
                                                  location, 
                                                  user_agent) 
                                                  
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           
                           ON CONFLICT DO NOTHING"""

user_table_insert = """INSERT INTO users (user_id, 
                                          first_name, 
                                          last_name, 
                                          gender, 
                                          level) 
                                          
                       VALUES (%s, %s, %s, %s, %s)
                       
                       ON CONFLICT (user_id) 
                       DO UPDATE 
                       SET level = EXCLUDED.level"""

song_table_insert = """INSERT INTO songs (song_id, 
                                          title, 
                                          artist_id, 
                                          year, 
                                          duration) 
                                          
                       VALUES (%s, %s, %s, %s, %s)
                       
                       ON CONFLICT DO NOTHING"""

artist_table_insert = """INSERT INTO artists (artist_id, 
                                              artist_name, 
                                              artist_location, 
                                              artist_latitude, 
                                              artist_longitude) 
                                              
                         VALUES (%s, %s, %s, %s, %s)
                         
                         ON CONFLICT DO NOTHING"""

time_table_insert = """INSERT INTO time (start_time, 
                                         hour, 
                                         day, 
                                         week, 
                                         month, 
                                         year, 
                                         weekday) 
                                         
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       
                       ON CONFLICT DO NOTHING"""

# FIND SONGS

song_select = """SELECT songs.song_id, artists.artist_id 
                 FROM songs 
                 JOIN artists 
                 ON songs.artist_id = artists.artist_id 
                 WHERE songs.title = %s 
                 AND artists.artist_name = %s 
                 AND songs.duration = %s""" 

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]