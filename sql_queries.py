import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    first_name      VARCHAR,
    gender          VARCHAR,
    item_in_session INTEGER,
    last_name       VARCHAR,
    length          REAL,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    REAL,
    session_id      INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    user_agent      VARCHAR,
    user_id         VARCHAR                              
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    song_id             VARCHAR,
    title               VARCHAR,
    duration            REAL,
    year                SMALLINT,
    artist_id           VARCHAR,
    artist_name         VARCHAR,
    artist_latitude     REAL,
    artist_longitude    REAL,
    artist_location     VARCHAR,
    num_songs           INTEGER
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id    BIGINT IDENTITY(1, 1) PRIMARY KEY,
    start_time     TIMESTAMP NOT NULL SORTKEY,
    user_id        VARCHAR NOT NULL DISTKEY,
    level          VARCHAR,
    song_id        VARCHAR,
    artist_id      VARCHAR,
    session_id     INTEGER,
    location       VARCHAR,
    user_agent     VARCHAR
) diststyle key;
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    user_id     VARCHAR PRIMARY KEY SORTKEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR
) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id     VARCHAR PRIMARY KEY SORTKEY,
    title       VARCHAR,
    artist_id   VARCHAR DISTKEY,
    year        SMALLINT,
    duration    REAL
) diststyle key;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id   VARCHAR PRIMARY KEY SORTKEY,
    name        VARCHAR,
    location    VARCHAR,
    latitude    REAL,
    longitude   REAL
) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time  TIMESTAMP PRIMARY KEY SORTKEY,
    hour        SMALLINT,
    day         SMALLINT,
    week        SMALLINT,
    month       SMALLINT,
    year        SMALLINT DISTKEY,
    weekday     SMALLINT
) diststyle key;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY {}
FROM {}
IAM_ROLE '{}'
JSON {}
REGION '{}';
""").format(
'staging_events',
config['S3']['LOG_DATA'],
config['IAM_ROLE']['ARN'],
config['S3']['LOG_JSONPATH'],
config['CLUSTER']['REGION']
)

staging_songs_copy = ("""
COPY {}
FROM {}
IAM_ROLE '{}'
JSON '{}'
REGION '{}';
""").format(
'staging_songs',
config['S3']['SONG_DATA'],
config['IAM_ROLE']['ARN'],
'auto',
config['CLUSTER']['REGION']
)

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
