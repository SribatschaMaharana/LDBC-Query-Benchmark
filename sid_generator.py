#3
import csv
import os
import sys

INDEX_MAP = "graph_outputs/vid_index_map.csv"
METIS_PARTITION = "graph_outputs/graph.txt.part.2"
OUTPUT_LOG = "partitioned_vids/vid_sid_log.csv"

index_to_vid = {}

# Load index → vid mapping
try:
    with open(INDEX_MAP, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                index = int(row["index"])
                vid = int(row["vid"])
                index_to_vid[index] = vid
            except (KeyError, ValueError):
                print(f"Skipping malformed row in {INDEX_MAP}: {row}", file=sys.stderr)
except FileNotFoundError:
    print(f"Error: {INDEX_MAP} not found.", file=sys.stderr)
    sys.exit(1)

# Read METIS partition assignments
try:
    with open(METIS_PARTITION) as f:
        sids = [int(line.strip()) for line in f if line.strip()]
except FileNotFoundError:
    print(f"Error: {METIS_PARTITION} not found.", file=sys.stderr)
    sys.exit(1)

# Check for index mismatch
if len(sids) != len(index_to_vid):
    print(f"Warning: Number of lines in METIS output ({len(sids)}) does not match index map ({len(index_to_vid)})", file=sys.stderr)

# Write vid → sid mapping
os.makedirs(os.path.dirname(OUTPUT_LOG), exist_ok=True)
with open(OUTPUT_LOG, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["vid", "sid"])

    missing = 0
    for i, sid in enumerate(sids):
        vid = index_to_vid.get(i + 1)  # METIS is 1-based
        if vid is not None:
            writer.writerow([vid, sid])
        else:
            print(f"Missing vid for index {i+1}", file=sys.stderr)
            missing += 1

print(f"Wrote {OUTPUT_LOG}")
if missing:
    print(f"Skipped {missing} entries due to missing vids.")
