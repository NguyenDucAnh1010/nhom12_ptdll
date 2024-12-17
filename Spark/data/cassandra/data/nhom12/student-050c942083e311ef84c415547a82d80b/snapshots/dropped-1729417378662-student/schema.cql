CREATE TABLE IF NOT EXISTS nhom12.student (
    idstudent text PRIMARY KEY,
    address text,
    email text,
    idclass text,
    iddepartment text,
    nameclass text,
    namedepartment text,
    namestudent text,
    phonenumber text
) WITH ID = 050c9420-83e3-11ef-84c4-15547a82d80b
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';
CREATE INDEX IF NOT EXISTS student_namestudent_idx ON nhom12.student (namestudent);
CREATE INDEX IF NOT EXISTS student_idclass_idx ON nhom12.student (idclass);
