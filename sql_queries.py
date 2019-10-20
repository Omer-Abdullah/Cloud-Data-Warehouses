import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events_table"
staging_songs_table_drop = "drop table if exists staging_songs_table"
songplay_table_drop = "drop table if exists songplay_table"
user_table_drop = "drop table if exists user_table"
song_table_drop = "drop table if exists song_table"
artist_table_drop = "drop table if exists artist_table"
time_table_drop = "drop table if exists time_table"

# CREATE TABLES

staging_events_table_create= ("""

    create table if not exists staging_events_table
    (
    artist varchar,
    auth varchar,
    first_name varchar,
    gender varchar,
    iteminsession smallint,
    last_name varchar,
    length float,
    level varchar,
    location varchar,
    method varchar,
    page varchar,
    registration varchar,
    session_id int,
    song varchar,
    status int,
    ts bigint,
    user_agent varchar,
    user_id int
    )
    diststyle even
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs_table
    (
    num_songs int,
    artist_id varchar,
    artist_latitude numeric,
    artist_longitude numeric,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
    )
    diststyle even
""")

songplay_table_create = ("""
    create table if not exists songplay_table
    (
    songplay_id int IDENTITY(0,1) PRIMARY KEY sortkey, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar , 
    song_id varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    session_id int , 
    location varchar , 
    user_agent varchar 
    )
""")

user_table_create = ("""
    create table if not exists user_table
    (
    user_id int PRIMARY KEY, 
    first_name varchar , 
    last_name varchar , 
    gender varchar , 
    level varchar 
    )
    diststyle all
""")

song_table_create = ("""
    create table if not exists song_table
    (
    song_id varchar PRIMARY KEY, 
    title varchar , 
    artist_id varchar NOT NULL, 
    year int , 
    duration float 
    )
    diststyle all
""")

artist_table_create = ("""
    create table if not exists artist_table
    (
    artist_id varchar PRIMARY KEY, 
    name varchar , 
    location varchar , 
    lattitude numeric , 
    longitude numeric
    )
    diststyle all
""")

time_table_create = ("""
    create table if not exists time_table
    (
    ts bigint,
    start_time timestamp, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int , 
    weekday int
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events_table from {}
    iam_role {}
    region 'us-west-2' 
    json {}
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    COPY staging_songs_table from {}
    iam_role {}
    region 'us-west-2' 
    json 'auto'
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES


user_table_insert = ("""
    insert into user_table
    (
    select distinct user_id, 
    first_name, 
    last_name,
    gender,
    level
    from staging_events_table
    where user_id is not null
    )
""")

song_table_insert = ("""
    insert into song_table
    (
    select distinct song_id, 
    title, 
    artist_id, 
    year, 
    duration
    from staging_songs_table
    )
    
""")

artist_table_insert = ("""
    insert into artist_table
    (
    select distinct artist_id ,
    artist_name,
    artist_location,
    artist_latitude,
    artist_longitude
    from staging_songs_table
    )
""")

songplay_table_insert = ("""
    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT
    TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1second' as start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
    FROM staging_events_table se, staging_songs_table ss
    WHERE se.page = 'NextSong'
    AND se.song = ss.title
    AND se.artist = ss.artist_name
    AND se.length = ss.duration
""")

time_table_insert = ("""
    INSERT INTO time_table(ts,start_time,hour,day,week,month,year,weekday)
    Select distinct ts
    ,start_time
    ,EXTRACT(HOUR FROM start_time) As t_hour
    ,EXTRACT(DAY FROM start_time) As t_day
    ,EXTRACT(WEEK FROM start_time) As t_week
    ,EXTRACT(MONTH FROM start_time) As t_month
    ,EXTRACT(YEAR FROM start_time) As t_year
    ,EXTRACT(DOW FROM start_time) As t_weekday
    FROM (
    SELECT distinct ts,'1970-01-01'::date + ts/1000 * interval '1 second' as start_time
    FROM staging_events_table)
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
