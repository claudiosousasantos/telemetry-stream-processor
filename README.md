# Telemetry Stream Processor

A Python script that simulates processing a live stream of cell tower telemetry: messages arrive one at a time, some are broken, and they don't all use the same format.

## How it works
- A generator (`tower_stream`) yields one message at a time with a short delay, imitating a live feed
- Messages that aren't valid JSON are caught and counted instead of crashing the program
- Two different message formats are converted into one common schema: a nested format (`tower_id`, `ts`, `metrics`, `location`) and a flat format (`id`, Unix `timestamp`, `latency`, `signal`)
- Valid records are collected into micro-batches of 3, and each batch is checked for alerts: latency over 100 ms or signal weaker than -90 dBm
- Any leftover records at the end of the stream are processed as a final smaller batch
- Reports how many bad messages were skipped and shows the latest reading per tower

## Example results (sample data)
- Batch 1 (T10, T11, T12): 🚨 T12 high latency (120 ms), 🚨 T12 weak signal (-95 dBm)
- Final batch (2 records): no alerts
- Skipped 1 bad message

## How to run
```bash
python stream_processor.py
```
The run takes about 2 seconds because of the simulated delay between messages.

## What I learned
- Using a generator with `yield` to produce data one item at a time
- Catching `json.JSONDecodeError` so one bad message doesn't stop the stream
- Normalizing different input formats into one schema, using `.get()` to read optional fields safely
- Converting both ISO timestamp strings and Unix timestamps with `pd.to_datetime()`
- Micro-batching: collecting records in a buffer and processing them in groups
- Remembering to process the leftover buffer after the loop ends
- Combining batches with `pd.concat()`

## Limitations
- The stream is simulated from a hard-coded list. A real system would read from a socket, message queue, or API.
- Timestamps in ISO format have no timezone, while Unix timestamps are in UTC, so mixing them assumes the ISO ones are UTC as well.
- All data is fictional sample data.

## Dependencies
Requires pandas:
```bash
pip install pandas
```
