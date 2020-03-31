import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(255),
    auth VARCHAR(255),
    firstName VARCHAR(100),
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR(100),
    length FLOAT,
    level VARCHAR(20),
    location VARCHAR(255),
    method VARCHAR(10),
    page VARCHAR(50),
    registration FLOAT,
    sessionId INT,
    song VARCHAR(255),
    status INT,
    ts BIGINT,
    userAgent VARCHAR(255),
    userId INT
);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs INT,
    artist_id VARCHAR(18),
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR(255),
    artist_name VARCHAR(255),
    song_id VARCHAR(18),
    title VARCHAR(255),
    duration FLOAT,
    year INT
);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(20),
    song_id VARCHAR(18) NOT NULL,
    artist_id VARCHAR(18) NOT NULL,
    session_id INT,
    location VARCHAR(255),
    user_agent VARCHAR(255)
);""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    gender VARCHAR(1),
    level VARCHAR(20) NOT NULL
);""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(18) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id VARCHAR(18) NOT NULL,
    year INT,
    duration FLOAT
);""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(18) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
);""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL,
    weekday VARCHAR(10) NOT NULL
);""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {}
    IAM_ROLE {}
    COMPUPDATE OFF REGION 'us-west-2'
    JSON {};
""").format(config.get("S3", "LOG_DATA"),
            config.get("IAM_ROLE", "ARN"),
            config.get("S3", "LOG_JSONPATH"))

staging_songs_copy = ("""
    COPY staging_songs from {}
    IAM_ROLE {}
    COMPUPDATE OFF REGION 'us-west-2'
    JSON 'auto' TRUNCATECOLUMNS;
""").format(config.get("S3", "SONG_DATA"), config.get("IAM_ROLE", "ARN"))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays
    (start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT
    TIMESTAMP 'epoch' + events.ts/1000 * interval '1 second',
    events.userId,
    events.level,
    songs.song_id,
    songs.artist_id,
    events.sessionId,
    events.location,
    events.userAgent
FROM staging_events AS events
JOIN staging_songs AS songs
    ON events.song = songs.title
    AND events.artist = songs.artist_name
    AND events.length = songs.duration
WHERE events.page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users
    (user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT
    DISTINCT events.userId,
    events.firstName,
    events.lastName,
    events.gender,
    events.level
FROM staging_events AS events
WHERE
    events.page = 'NextSong'
    AND events.userId IS NOT NULL
    AND events.firstName IS NOT NULL
    AND events.lastName IS NOT NULL
    AND events.gender IS NOT NULL;
""")

song_table_insert = ("""INSERT INTO songs
    (song_id,
    title,
    artist_id,
    year,
    duration)
SELECT
    DISTINCT songs.song_id,
    songs.title,
    songs.artist_id,
    songs.year,
    songs.duration
FROM staging_songs AS songs
WHERE
    songs.song_id IS NOT NULL;
""")

artist_table_insert = ("""INSERT INTO artists
    (artist_id,
    name,
    location,
    latitude,
    longitude)
SELECT
    DISTINCT songs.artist_id,
    songs.artist_name,
    songs.artist_location,
    songs.artist_latitude,
    songs.artist_longitude
FROM staging_songs AS songs;
""")


time_table_insert = ("""INSERT INTO time
    (start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT
    start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    EXTRACT(DOW FROM start_time) AS weekday
FROM (SELECT
    DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time
FROM staging_events);
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]