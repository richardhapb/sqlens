CREATE TABLE IF NOT EXISTS queries (
    id serial PRIMARY KEY,
    query text UNIQUE,
    count bigint,
    total_duration real,
    min_duration real,
    max_duration real,
    avg_duration real
);

