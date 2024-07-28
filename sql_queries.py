import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get('IAM_ROLE','arn')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_JSON_PATH = config.get('S3','LOG_JSON_PATH')
SONG_DATA = config.get('S3','SONG_DATA')
REGION = "'"+ config.get('AWS','region') +  "'"

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR,
    gender CHAR(1),
    itemInSession INTEGER,
    lastName VARCHAR,
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INTEGER,
        artist_id VARCHAR,
        artist_latitude FLOAT,
        artist_longitude FLOAT,
        artist_location VARCHAR,
        artist_name VARCHAR,
        song_id VARCHAR,
        title VARCHAR,
        duration FLOAT,
        year INTEGER);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id INTEGER IDENTITY(1, 1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR,
    song_id VARCHAR,
    artist_id VARCHAR,
    session_id INTEGER,
    location VARCHAR,
    user_agent VARCHAR);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER NOT NULL PRIMARY KEY,
    first_name VARCHAR,
    last_name VARCHAR,
    gender CHAR(1),
    level VARCHAR);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR NOT NULL PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INTEGER,
    duration FLOAT);    
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR,
    location VARCHAR ,
    latitude FLOAT ,
    longitude FLOAT);    
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY,
    hour INTEGER,
    day INTEGER,
    week INTEGER,
    month INTEGER,
    year INTEGER,
    weekday VARCHAR);    
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events 
    from {} 
    credentials 'aws_iam_role={}'   
    format as json {} 
    compupdate off 
    region {};
""").format(LOG_DATA, DWH_ROLE_ARN, LOG_JSON_PATH, REGION)

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    credentials 'aws_iam_role={}' 
    format as json 'auto'     
    compupdate off 
    region {};
""").format(SONG_DATA, DWH_ROLE_ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT
    timestamp 'epoch' + se.ts/1000 * interval '1 second',
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent 
FROM staging_events se
JOIN staging_songs ss ON (se.artist = ss.artist_name AND se.song = ss.title)
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT DISTINCT
    user_id,
    first_name,
    last_name,
    gender,
    level
FROM (
    SELECT
        se.userId AS user_id,
        se.firstName AS first_name,
        se.lastName AS last_name,
        se.gender AS gender,
        se.level AS level,
        ROW_NUMBER() OVER (PARTITION BY se.userId ORDER BY se.ts DESC) AS rnk
    FROM staging_events se
    WHERE se.userId IS NOT NULL
) subquery
WHERE rnk = 1;
""")

# user_table_insert = ("""
#     INSERT INTO users (
#         user_id,
#         first_name,
#         last_name,
#         gender,
#         level)
#     SELECT DISTINCT
#         se.userId,
#         se.firstName,
#         se.lastName,
#         se.gender,
#         se.level
#     FROM staging_events se
#     WHERE se.userId IS NOT NULL;
# """)

song_table_insert = ("""
    INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration)
SELECT DISTINCT
    ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM staging_songs ss;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT DISTINCT
    artist_id,
    name,
    location,
    latitude,
    longitude
FROM (
    SELECT
        ss.artist_id,
        ss.artist_name AS name,
        ss.artist_location AS location,
        CAST(ss.artist_latitude AS double precision) AS latitude,
        CAST(ss.artist_longitude AS double precision) AS longitude,
        ROW_NUMBER() OVER (PARTITION BY ss.artist_id ORDER BY ss.year DESC) AS rnk
    FROM staging_songs ss
) subquery
WHERE rnk = 1;
""")

# artist_table_insert = ("""
# INSERT INTO artists (
#     artist_id,
#     name,
#     location,
#     latitude,
#     longitude)
# SELECT DISTINCT
#     ss.artist_id,
#     ss.artist_name,
#     ss.artist_location,
#     CAST(ss.artist_latitude AS double precision),
#     CAST(ss.artist_longitude AS double precision)
# FROM staging_songs ss;
# """)

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT DISTINCT
    start_time,
    EXTRACT(HOUR FROM start_time),
    EXTRACT(DAY FROM start_time),
    EXTRACT(WEEK FROM start_time),
    EXTRACT(MONTH FROM start_time),
    EXTRACT(YEAR FROM start_time),
    EXTRACT(DOW FROM start_time)
FROM songplay;
""")

# DATA INTEGRITY CHECKS
check_duplicates_songplay = """
SELECT songplay_id, COUNT(*)
FROM songplay
GROUP BY songplay_id
HAVING COUNT(*) > 1;
"""

check_duplicates_users = """
SELECT user_id, COUNT(*)
FROM users
GROUP BY user_id
HAVING COUNT(*) > 1;
"""

check_duplicates_songs = """
SELECT song_id, COUNT(*)
FROM songs
GROUP BY song_id
HAVING COUNT(*) > 1;
"""

check_duplicates_artists = """
SELECT artist_id, COUNT(*)
FROM artists
GROUP BY artist_id
HAVING COUNT(*) > 1;
"""

check_duplicates_time = """
SELECT start_time, COUNT(*)
FROM time
GROUP BY start_time
HAVING COUNT(*) > 1;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
check_duplicates_queries = [check_duplicates_songplay, check_duplicates_users, check_duplicates_songs, check_duplicates_artists, check_duplicates_time]
