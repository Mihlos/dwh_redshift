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
    songplay_id       IDENTITY(0,1), 
    start_time        DOUBLE PRECISION NOT NULL, 
    user_id           SMALLINT NOT NULL, 
    level             VARCHAR(20) NOT NULL, 
    song_id           CHAR(20), 
    artist_id         CHAR(20), 
    session_id        CHAR(20), 
    location          VARCHAR(80), 
    user_agent        VARCHAR(80),
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user
(
    user_id          SMALLINT, 
    first_name       VARCHAR(80) NOT NULL, 
    last_name        VARCHAR(80) NOT NULL,
    gender           VARCHAR(80), 
    level            CHAR(20),
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song
(   
    song_id          CHAR(20), 
    title            VARCHAR(80) NOT NULL, 
    artist_id        CHAR(20), 
    year             SMALLINT, 
    duration         DOUBLE PRECISION NOT NULL,
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist 
(
    artist_id        CHAR(20), 
    name             VARCHAR(80) NOT NULL, 
    location         VARCHAR(80), 
    latitude         DOUBLE PRECISION, 
    longitude        DOUBLE PRECISION,
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time       DOUBLE PRECISION, 
    hour             SMALLINT, 
    day              SMALLINT, 
    week             SMALLINT, 
    month            SMALLINT, 
    year             SMALLINT, 
    weekday          SMALLINT,
);
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    gzip region 'us-west-2';
    """).format(SONG_DATA, )

staging_songs_copy = ("""
    COPY staging_songs FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    gzip region 'us-west-2';
    """).format(LOG_DATA, )


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
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
