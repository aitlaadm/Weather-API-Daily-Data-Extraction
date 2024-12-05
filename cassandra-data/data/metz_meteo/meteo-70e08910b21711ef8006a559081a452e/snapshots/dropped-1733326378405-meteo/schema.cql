CREATE TABLE IF NOT EXISTS metz_meteo.meteo (
    id uuid PRIMARY KEY,
    all int,
    base text,
    cod int,
    country text,
    deg text,
    description text,
    df timestamp,
    feels_like float,
    humidity int,
    icon text,
    id_meteo text,
    lat text,
    lon text,
    main text,
    name text,
    pressure text,
    sea_level int,
    speed float,
    sunrise date,
    sunset date,
    temp float,
    temp_max float,
    temp_min float,
    timezone int,
    visibility int
) WITH ID = 70e08910-b217-11ef-8006-a559081a452e
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = 'default'
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
