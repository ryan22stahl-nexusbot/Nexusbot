# Layer 1: Data Spine

Ingests historical and live tick data from Databento into QuestDB.

## Setup
1. `pip install databento questdb pandas python-dotenv`
2. Set `DATABENTO_API_KEY` in your environment (or `.env`).
3. Run QuestDB locally: `docker run -d -p 9000:9000 -p 8812:8812 questdb/questdb`
4. `python layer1/ingest.py --symbol ES --start 2024-01-01 --end 2024-01-02`

## Schema
- Table: `trades`
- Columns: ts_event (timestamp), symbol (symbol), price (double), size (long), side (symbol), source (symbol)
- Designated timestamp: ts_event

## Replay
Every record carries a sequence number and checksum for exact replay.
