-- QuestDB DDL for the trades table (Layer 1)
-- Run once: http://localhost:9000/exec?query=...

CREATE TABLE IF NOT EXISTS trades (
    ts_event TIMESTAMP,
    symbol SYMBOL INDEX,
    price DOUBLE,
    size LONG,
    side SYMBOL,
    source SYMBOL,
    seq LONG,
    checksum SYMBOL
) TIMESTAMP(ts_event) PARTITION BY DAY;
