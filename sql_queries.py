import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA               = config.get("S3","LOG_DATA")
LOG_JSONPATH           = config.get("S3","LOG_JSONPATH")
SONG_DATA              = config.get("S3","SONG_DATA")
ARN                    = config.get('IAM_ROLE', 'ARN')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"


# CREATE TABLES
staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist            VARCHAR(80),
    auth              VARCHAR(40),
    firstName         VARCHAR(80),
    gender            CHAR(12),
    itemInSession     SMALLINT,
    lastName          VARCHAR(80),
    length            DOUBLE PRECISION,
    level             CHAR(20),
    location          VARCHAR(80),
    method            CHAR(10),
    page              CHAR(20),
    registration      DOUBLE PRECISION,
    sessionId         INTEGER,
    song              VARCHAR(80),
    status            SMALLINT,
    ts                BIGINT,
    userAgent         TEXT,
    userId            INTEGER
)DISTSTYLE EVEN;
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
)DISTSTYLE EVEN;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id       INT IDENTITY(0,1), 
    start_time        DOUBLE PRECISION NOT NULL, 
    user_id           SMALLINT NOT NULL, 
    level             VARCHAR(20) NOT NULL, 
    song_id           CHAR(20), 
    artist_id         CHAR(20), 
    session_id        CHAR(20), 
    location          VARCHAR(80), 
    user_agent        TEXT,
    PRIMARY KEY       (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id          SMALLINT, 
    first_name       VARCHAR(80) NOT NULL, 
    last_name        VARCHAR(80) NOT NULL,
    gender           VARCHAR(80), 
    level            CHAR(20),
    PRIMARY KEY      (user_id)
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(   
    song_id          CHAR(20), 
    title            VARCHAR(80) NOT NULL, 
    artist_id        CHAR(20), 
    year             SMALLINT, 
    duration         DOUBLE PRECISION,
    PRIMARY KEY      (song_id)
    
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id        CHAR(20), 
    name             VARCHAR(80) NOT NULL, 
    location         VARCHAR(80), 
    latitude         DOUBLE PRECISION, 
    longitude        DOUBLE PRECISION,
    PRIMARY KEY      (artist_id)
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time       BIGINT, 
    hour             SMALLINT, 
    day              SMALLINT, 
    week             SMALLINT, 
    month            SMALLINT, 
    year             SMALLINT, 
    weekday          SMALLINT,
    PRIMARY KEY      (start_time)
);
""")


# STAGING TABLES
staging_events_copy = ("""
    COPY staging_events 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    FORMAT AS json '{}' region 'us-west-2';
    """).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs 
    FROM '{}'
    CREDENTIALS 'aws_iam_role={}'
    format as json 'auto' region 'us-west-2';
    """).format(SONG_DATA, ARN)


# FINAL TABLES
songplay_table_insert = ("""
""")

user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT(userID) AS user_id, 
       firstName        AS first_name, 
       lastName         AS last_name, 
       gender           AS gender, 
       level            AS level
    FROM staging_events
    WHERE page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, 
       title, 
       artist_id, 
       year, 
       duration
    FROM staging_songs
""")
    
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT(artist_id)   AS artist_id, 
       artist_name           AS name,  
       artist_location       AS location, 
       artist_latitude       AS latitude, 
       artist_longitude      AS longitude
    FROM staging_songs
""")

time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT  ts                                  AS start_time,
        EXTRACT(HOUR FROM t_start_time)     AS hour,
        EXTRACT(DAY FROM t_start_time)      AS day,
        EXTRACT(WEEK FROM t_start_time)     AS week,
        EXTRACT(MONTH FROM t_start_time)    AS month,
        EXTRACT(YEAR FROM t_start_time)     AS year,
        EXTRACT(DOW FROM t_start_time)      AS weekday
    FROM (
        SELECT DISTINCT(ts),
               page,
               '1970-01-01'::date + ts/1000 * interval '1 second' AS t_start_time
            FROM staging_events
            WHERE page = 'NextSong' 
    ) AS stg_events
""")

# QUERY LISTS
create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [#songplay_table_insert, 
                        user_table_insert, 
                        song_table_insert, 
                        artist_table_insert, 
                        time_table_insert]