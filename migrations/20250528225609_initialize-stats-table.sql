CREATE TABLE IF NOT EXISTS queries (
    id SERIAL PRIMARY KEY,
    query varchar(3000) UNIQUE,
    count bigint,
    total_duration real,
    min_duration real,
    max_duration real,
    avg_duration real
);

