# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE songplays (
songplay_id int, start_time bigint, user_id int, level text, song_id text, artist_id text, session_id int, location text, user_agent text)
""")

user_table_create = ("""CREATE TABLE users (
user_id int, first_name text, last_name text, gender text, level text)
""")

song_table_create = ("""CREATE TABLE songs
( song_id text, title text, artist_id text, year int, duration numeric)
""")

artist_table_create = ("""CREATE TABLE artists
(artist_id text, name text, location text, latitude numeric, longitude numeric)
""")

time_table_create = ("""CREATE TABLE time
(start_time bigint, hour int, day int, week int, month int, year int, weekday int)
""")

# INSERT RECORDS

songplay_table_insert = ("""INSERT INTO songplays (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
	values (%s, %s, %s, %s, %s)
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
values (%s, %s, %s, %s, %s)""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
values (%s, %s, %s, %s, %s)""")


time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
values(%s, %s, %s, %s, %s, %s, %s)""")

# FIND SONGS

song_select = ("""select s.song_id, a.artist_id from songs s 
inner join artists a on s.artist_id = a.artist_id
where s.title = %s and a.name = %s and s.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]