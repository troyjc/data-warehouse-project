import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

song_data = 's3://udacity-dend/song_data/'
log_data = 's3://udacity-dend/log_data/'
jsonpaths = 's3://udacity-dend/log_json_path.json'

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_songs_table_create= ("""CREATE TABLE IF NOT EXISTS songs (artist_id text NOT NULL,
                                                                  artist_latitude float,
                                                                  artist_longitude float,
                                                                  artist_location text,
                                                                  artist_name text NOT NULL,
                                                                  song_id text NOT NULL,
                                                                  title text NOT NULL,
                                                                  duration numeric(9, 5),
                                                                  year integer);
""")

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS events (artist text,
                                                                     auth text NOT NULL,
                                                                     first_name text,
                                                                     gender char,
                                                                     item_in_session integer NOT NULL,
                                                                     last_name text,
                                                                     length numeric(10,5),
                                                                     level text NOT NULL,
                                                                     location text,
                                                                     method text NOT NULL,
                                                                     page text NOT NULL,
                                                                     registration numeric(14, 1),
                                                                     session_id integer NOT NULL,
                                                                     song text,
                                                                     status char(3) NOT NULL,
                                                                     ts bigint NOT NULL,
                                                                     user_agent varchar(512),
                                                                     user_id integer);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay (songplay_id bigint IDENTITY(0, 1),
                                                                 start_time timestamp NOT NULL,
                                                                 user_id integer NOT NULL,
                                                                 level text NOT NULL,
                                                                 song_id text NOT NULL,
                                                                 artist_id text NOT NULL,
                                                                 session_id integer NOT NULL,
                                                                 location text NOT NULL,
                                                                 user_agent varchar(512) NOT NULL,

                                                                 foreign key(user_id) references users (user_id),
                                                                 foreign key(song_id) references song (song_id),
                                                                 foreign key(artist_id) references artist (artist_id))
                            DISTSTYLE KEY DISTKEY (user_id);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id integer NOT NULL PRIMARY KEY,
                                                          first_name text NOT NULL,
                                                          last_name text NOT NULL,
                                                          gender char,
                                                          level text NOT NULL)
                        DISTSTYLE KEY DISTKEY (user_id);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song (song_id text NOT NULL PRIMARY KEY,
                                                         title text NOT NULL,
                                                         artist_id text NOT NULL,
                                                         year integer,
                                                         duration numeric(10, 5),

                                                         foreign key(artist_id) references artist (artist_id));
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist (artist_id text NOT NULL PRIMARY KEY,
                                                             name text NOT NULL,
                                                             location text,
                                                             latitude float,
                                                             longitude float)
                          DISTSTYLE ALL;
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time timestamp NOT NULL PRIMARY KEY,
                                                         hour integer,
                                                         day integer,
                                                         week integer,
                                                         month integer,
                                                         year integer,
                                                         weekday integer);
""")

# STAGING TABLES

staging_songs_copy = ("""COPY songs FROM '{}' iam_role '{}' FORMAT JSON 'auto';
""").format(song_data, config['DWH']['DWH_IAM_ROLE'])

staging_events_copy = ("""COPY events FROM '{}' iam_role '{}' FORMAT JSON '{}';

                          -- Filter by actions for song plays
                          DELETE FROM events
                          WHERE page <> 'NextSong';
""").format(log_data, config['DWH']['DWH_IAM_ROLE'], jsonpaths)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                            SELECT TIMESTAMP 'epoch' + ts * INTERVAL '0.001 second' AS start_time,
                                   user_id,
                                   level,
                                   song_id,
                                   artist_id,
                                   session_id,
                                   location,
                                   user_agent
                            FROM events
                            INNER JOIN song
                                ON events.song = song.title;
""")

user_table_insert = ("""-- Use a temporary staging table to remove duplicates before doing a MERGE.
                        -- Duplicate simply means that the artist_id key is the same
                        CREATE TEMP TABLE staging_users (LIKE users);

                        INSERT INTO staging_users
                        SELECT user_id, first_name, last_name, gender, level
                        FROM (
                            SELECT user_id,
                                   first_name,
                                   last_name,
                                   gender,
                                   level,
                                   ROW_NUMBER() OVER (PARTITION BY user_id
                                                      ORDER BY level DESC) AS user_id_ranked
                            FROM events) AS ranked
                        WHERE ranked.user_id_ranked = 1;

                        -- Perform a MERGE operation by replacing existing rows
                        DELETE from users
                        USING staging_users
                        WHERE users.user_id = staging_users.user_id;

                        -- Insert all the rows from the staging table into the target table
                        INSERT INTO users
                        SELECT * FROM staging_users;

                       DROP TABLE staging_users;
""")

song_table_insert = ("""CREATE TEMP TABLE staging_song (LIKE song);

                        INSERT INTO staging_song
                        SELECT DISTINCT song_id, title, artist_id, year, duration
                        FROM songs; 

                        -- Perform a MERGE operation by replacing existing rows
                        DELETE from song
                        USING staging_song
                        WHERE song.song_id = staging_song.song_id;

                        -- Insert all the rows from the staging table into the target table
                        INSERT INTO song
                        SELECT * FROM staging_song;

                        DROP TABLE staging_song;
""")

artist_table_insert = ("""-- Use a temporary staging table to remove duplicates before doing a MERGE.
                          -- Duplicate simply means that the artist_id key is the same
                          CREATE TEMP TABLE staging_artist (LIKE artist);

                          INSERT INTO staging_artist
                          SELECT artist_id, name, location, latitude, longitude
                          FROM (
                              SELECT artist_id,
                                     artist_name AS name,
                                     artist_location AS location,
                                     artist_latitude AS latitude,
                                     artist_longitude AS longitude,
                                     ROW_NUMBER() OVER (PARTITION BY artist_id
                                                        ORDER BY name, location) AS artist_id_ranked
                              FROM songs) AS ranked
                          WHERE ranked.artist_id_ranked = 1;

                          -- Perform a MERGE operation by replacing existing rows
                          DELETE FROM artist
                          USING staging_artist
                          WHERE artist.artist_id = staging_artist.artist_id;

                          -- Insert all the rows from the staging table into the target table
                          INSERT INTO artist
                          SELECT * FROM staging_artist;

                         DROP TABLE staging_artist;
""")

time_table_insert = ("""CREATE TEMP TABLE staging_time (start_time timestamp);

                        INSERT INTO staging_time
                        SELECT DISTINCT TIMESTAMP 'epoch' + ts * INTERVAL '0.001 second' AS start_time
                        FROM events;

                        -- Perform a MERGE operation by replacing existing rows
                        DELETE FROM time
                        USING staging_time
                        WHERE time.start_time = staging_time.start_time;

                        -- Insert all the rows from the staging table into the target table
                        INSERT INTO time
                        SELECT start_time,
                               EXTRACT ('hour' FROM start_time),
                               EXTRACT ('day' FROM start_time),
                               EXTRACT ('week' FROM start_time),
                               EXTRACT ('month' FROM start_time),
                               EXTRACT ('year' FROM start_time),
                               EXTRACT ('dayofweek' FROM start_time)
                        FROM staging_time;

                        DROP TABLE staging_time;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [song_table_insert, user_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]
