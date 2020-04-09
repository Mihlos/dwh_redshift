import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

LOG_DATA               = config.get("S3","LOG_DATA")
LOG_JSONPATH           = config.get("S3","LOG_JSONPATH")
SONG_DATA              = config.get("S3","SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS user"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs         SMALLINT NOT NULL,
    artist_id         CHAR(20) NOT NULL,
    artist_latitude   DOUBLE PRECISION,
    artist_longitude  DOUBLE PRECISION,
    artist_location   VARCHAR(80),
    artist_name       VARCHAR(80),
    song_id           CHAR(20) NOT NULL,
    title             VARCHAR(80) NOT NULL,
    duration          DOUBLE PRECISION NOT NULL,
    year              SMALLINT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay
(
    songplay_id, 
    start_time, 
    user_id, 
    level, 
    song_id, 
    artist_id, 
    session_id, 
    location, 
    user_agent
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user
(
    user_id, 
    first_name, 
    last_name, 
    gender, 
    level
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(   
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist 
(
    artist_id, 
    name, 
    location, 
    lattitude, 
    longitude
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
gzip region 'us-west-2';
""").format(SONG_DATA, )

staging_songs_copy = ("""
COPY staging_songs FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role={}'
gzip region 'us-west-2';
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_songs_table_create]
drop_table_queries = [staging_songs_table_create] 

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
# copy_table_queries = [staging_events_copy, staging_songs_copy]
# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
