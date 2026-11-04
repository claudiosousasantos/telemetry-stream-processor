import json
import time
import pandas as pd

raw_messages = [
    '{"tower_id": "T10", "ts": "2024-01-15T08:00:00", "metrics": {"latency_ms": 35, "signal_dbm": -70}, "location": {"city": "Sao Paulo", "lat": -23.55, "lon": -46.63}}',
    '{"tower_id": "T11", "ts": "2024-01-15T08:00:02", "metrics": {"latency_ms": 62, "signal_dbm": -85}, "location": {"city": "Aruja", "lat": -23.39, "lon": -46.32}}',
    'this is not valid json {{{',
    '{"id": "T12", "timestamp": 1705305605, "latency": 120, "signal": -95}',
    '{"tower_id": "T10", "ts": "2024-01-15T08:00:05", "metrics": {"latency_ms": 38, "signal_dbm": -71}, "location": {"city": "Sao Paulo", "lat": -23.55, "lon": -46.63}}',
    '{"tower_id": "T13", "ts": "2024-01-15T08:00:07", "metrics": {"latency_ms": null, "signal_dbm": -80}, "location": {"city": "Santos", "lat": -23.96, "lon": -46.33}}',
]

def tower_stream():
    for message in raw_messages:
        time.sleep(0.3)
        yield message

def normalize(record):
    if "tower_id" in record:
        metrics = record.get("metrics", {})
        location = record.get("location", {})
        return {
            "tower_id": record["tower_id"],
            "timestamp": pd.to_datetime(record["ts"]),
            "latency_ms": metrics.get("latency_ms"),
            "signal_dbm": metrics.get("signal_dbm"),
            "city": location.get("city"),
        }
    if "id" in record:
        return {
            "tower_id": record["id"],
            "timestamp": pd.to_datetime(record["timestamp"], unit="s"),
            "latency_ms": record.get("latency"),
            "signal_dbm": record.get("signal"),
            "city": None,
        }
    return None

def check_alerts(batch):
    for _, row in batch.iterrows():
        if row["latency_ms"] > 100:
            print(f"   🚨 {row['tower_id']}: high latency ({row['latency_ms']} ms)")
        if row["signal_dbm"] < -90:
            print(f"   🚨 {row['tower_id']}: weak signal ({row['signal_dbm']} dBm)")

batch_size = 3
buffer = []
batches = []
bad_count = 0

for raw in tower_stream():
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        bad_count += 1
        continue

    clean = normalize(record)
    if clean is None:
        bad_count += 1
        continue

    buffer.append(clean)

    if len(buffer) == batch_size:
        batch_df = pd.DataFrame(buffer)
        print(f"\nBatch of {len(batch_df)} records received:")
        check_alerts(batch_df)
        batches.append(batch_df)
        buffer = []

if len(buffer) > 0:
    batch_df = pd.DataFrame(buffer)
    print(f"\nFinal batch of {len(batch_df)} records:")
    check_alerts(batch_df)
    batches.append(batch_df)

print(f"\nSkipped {bad_count} bad message(s)")

final_df = pd.concat(batches, ignore_index=True)
print("\nAll normalized records:")
print(final_df)

print("\nLatest reading per tower:")
print(final_df.sort_values("timestamp").groupby("tower_id").last())