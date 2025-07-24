import csv
from collections import defaultdict

ORG_MAP = "partitioned_vids/organisation_vid_map.csv"
PLACE_MAP = "partitioned_vids/place_vid_map.csv"

REL_FILE = "import/social_network/static/organisation_isLocatedIn_place_0_0.csv"

def load_vid_map(path):
    with open(path) as f:
        reader = csv.DictReader(f)
        return {int(row["original_id"]): int(row["vid"]) for row in reader}

org_map = load_vid_map(ORG_MAP)
place_map = load_vid_map(PLACE_MAP)

#list: vid -> set(neighbor vids)
edges = defaultdict(set)

with open(REL_FILE, newline='') as f:
    reader = csv.reader(f, delimiter="|")
    next(reader)
    for row in reader:
        try:
            org_id, place_id = int(row[0]), int(row[1])
            org_vid = org_map[org_id]
            place_vid = place_map[place_id]

            edges[org_vid].add(place_vid)
            edges[place_vid].add(org_vid)
        except Exception as e:
            print("skip row:", row, "| err:", e)
#

all_vids = sorted(edges.keys())
vid_to_index = {vid: idx+1 for idx, vid in enumerate(all_vids)}
index_to_vid = {v: k for k, v in vid_to_index.items()}

with open("graph.txt", "w") as f:
    f.write(f"{len(all_vids)} {sum(len(n) for n in edges.values()) // 2}\n")
    for vid in all_vids:
        neighbors = sorted(vid_to_index[nbr] for nbr in edges[vid])
        f.write(" ".join(map(str, neighbors)) + "\n")
