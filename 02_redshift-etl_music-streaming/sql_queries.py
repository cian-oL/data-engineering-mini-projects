import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

S3_REGION = config.get("S3", "REGION")
S3_LOG_DATA = config.get("S3", "LOG_DATA")
S3_LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
S3_SONG_DATA = config.get("S3", "SONG_DATA")

ROLE_ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender CHAR(1),
    itemInSession INT,
    lastName TEXT,
    length DECIMAL(10,5),
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration BIGINT,
    sessionId INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
)
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id TEXT,
    title TEXT,
    duration DECIMAL(10,5),
    year INT,
    artist_id TEXT,
    artist_name TEXT,
    artist_location TEXT,
    artist_latitude DECIMAL(9,6),
    artist_longitude DECIMAL(9,6)
)
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS fact_songplays (
    songplay_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level TEXT,
    song_id TEXT NOT NULL DISTKEY,
    artist_id TEXT NOT NULL,
    session_id INT,
    location TEXT,
    user_agent TEXT
)
COMPOUND SORTKEY(start_time, session_id)
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS dim_users (
    user_id INT PRIMARY KEY SORTKEY,
    first_name TEXT,
    last_name TEXT,
    gender CHAR(1),
    level TEXT
)
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS dim_songs (
    song_id TEXT PRIMARY KEY DISTKEY,
    title TEXT,
    artist_id TEXT NOT NULL,
    year INT SORTKEY,
    duration DECIMAL(10,5)
)
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS dim_artists (
    artist_id TEXT PRIMARY KEY SORTKEY,
    name TEXT,
    location TEXT,
    latitude DECIMAL(9,6),
    longitude DECIMAL(9,6)
)
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS dim_time (
    start_time TIMESTAMP PRIMARY KEY SORTKEY,
    hour SMALLINT,
    day SMALLINT,
    week SMALLINT,
    month SMALLINT,
    year INT,
    weekday SMALLINT
)
"""

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events
    FROM '{}'
    REGION '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON '{}'
    """
).format(S3_LOG_DATA, S3_REGION, ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = (
    """
    COPY staging_songs 
    FROM '{}'
    REGION '{}'
    IAM_ROLE '{}'
    FORMAT AS JSON 'auto'
    """
).format(S3_SONG_DATA, S3_REGION, ROLE_ARN)

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO fact_songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT
        TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time, 
        events.userId,
        events.level,
        songs.song_id,
        songs.artist_id,
        events.sessionId,
        events.location,
        events.userAgent
    FROM staging_events events
    LEFT JOIN staging_songs songs
        ON events.song = songs.title 
        AND events.artist = songs.artist_name
    WHERE songs.song_id IS NOT NULL AND events.page = 'NextSong'
    """

user_table_insert = """
    INSERT INTO dim_users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE userId IS NOT NULL AND events.page = 'NextSong'
    """

song_table_insert = """
    INSERT INTO dim_songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    """

artist_table_insert = """
    INSERT INTO dim_artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT DISTINCT
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    """

time_table_insert = """
    INSERT INTO dim_time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT 
        TIMESTAMP 'epoch' + events.ts/1000 * INTERVAL '1 second' AS start_time,
        EXTRACT(hour from start_time),
        EXTRACT(day from start_time),
        EXTRACT(week from start_time),
        EXTRACT(month from start_time),
        EXTRACT(year from start_time),
        EXTRACT(weekday from start_time)
    FROM staging_events events
    WHERE events.page = 'NextSong'
    """

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create,
]

drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop,
]

copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
]
