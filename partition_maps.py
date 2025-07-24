#2 generate graph.txt
import os
import csv
from collections import defaultdict

SOCIAL_NETWORK_DIR = "import/social_network"
PARTITIONED_VID_DIR = "partitioned_vids"
GRAPH_OUTPUT_DIR = "graph_outputs"
GRAPH_OUTPUT_FILE = os.path.join(GRAPH_OUTPUT_DIR, "graph.txt")
#INDEX_MAP_FILE = os.path.join(GRAPH_OUTPUT_DIR, "vid_index_map.csv")

# Relationships to include in the graph
RELATIONSHIPS = [
    ("organisation", "place", "organisation_isLocatedIn_place_0_0.csv"),
    ("person", "person", "person_knows_person_0_0.csv"),
    ("forum", "person", "forum_hasModerator_person_0_0.csv"),
    ("person", "place", "person_isLocatedIn_place_0_0.csv"),
    ("forum", "tag", "forum_hasTag_tag_0_0.csv"),
    ("tag", "tagclass", "tag_hasType_tagclass_0_0.csv"),
]

def load_vid_map(label):
    path = os.path.join(PARTITIONED_VID_DIR, f"{label}_vid_map.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"VID map not found: {path}")
    
    vid_map = {}
    with open(path, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                original_id = int(row["original_id"])
                vid = int(row["vid"])
                vid_map[original_id] = vid
            except Exception:
                continue
    return vid_map

def main():
    os.makedirs(GRAPH_OUTPUT_DIR, exist_ok=True)

    #  unique labels from relationships
    labels = set()
    for src, dst, _ in RELATIONSHIPS:
        labels.add(src)
        labels.add(dst)

    # Load vid maps
    vid_maps = {}
    for label in labels:
        try:
            vid_maps[label] = load_vid_map(label)
        except Exception as e:
            print(f"Error loading VID map for {label}: {e}")
            return

    edges = defaultdict(set)
    all_vids = set()

    # edges
    for src_label, dst_label, rel_file in RELATIONSHIPS:
        rel_path = None
        for subfolder in ["static", "dynamic"]:
            candidate_path = os.path.join(SOCIAL_NETWORK_DIR, subfolder, rel_file)
            if os.path.exists(candidate_path):
                rel_path = candidate_path
                break

        if rel_path is None:
            print(f"Skipping missing relationship file: {rel_file} (not found in static/ or dynamic/)")
            continue

        with open(rel_path, newline='') as f:
            reader = csv.reader(f, delimiter="|")
            try:
                next(reader)
            except StopIteration:
                continue

            for row in reader:
                try:
                    src_id = int(row[0])
                    dst_id = int(row[1])
                    src_vid = vid_maps[src_label][src_id]
                    dst_vid = vid_maps[dst_label][dst_id]

                    edges[src_vid].add(dst_vid)
                    edges[dst_vid].add(src_vid)
                    all_vids.update([src_vid, dst_vid])
                except Exception:
                    continue

    all_vids = sorted(all_vids)
    vid_to_index = {vid: i + 1 for i, vid in enumerate(all_vids)}


    index_map_path = os.path.join(GRAPH_OUTPUT_DIR, "vid_index_map.csv")
    with open(index_map_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["vid", "index"])
        for vid, index in vid_to_index.items():
            writer.writerow([vid, index])

    with open(GRAPH_OUTPUT_FILE, "w") as f:
        num_nodes = len(all_vids)
        num_edges = sum(len(nbrs) for nbrs in edges.values()) // 2
        f.write(f"{num_nodes} {num_edges}\n")

        for vid in all_vids:
            neighbors = edges.get(vid, set())
            neighbor_indices = sorted(vid_to_index[nbr] for nbr in neighbors if nbr in vid_to_index)
            f.write(" ".join(map(str, neighbor_indices)) + "\n")

    print(f"graph.txt written to {GRAPH_OUTPUT_FILE} with {num_nodes} nodes and {num_edges} edges")
    print(f"vid_index_map.csv written to {index_map_path}")

if __name__ == "__main__":
    main()


#     # Remap vids to metis 
#     all_vids = sorted(all_vids)
#     vid_to_index = {vid: i + 1 for i, vid in enumerate(all_vids)}

#     with open(GRAPH_OUTPUT_FILE, "w") as f:
#         num_nodes = len(all_vids)
#         num_edges = sum(len(nbrs) for nbrs in edges.values()) // 2
#         f.write(f"{num_nodes} {num_edges}\n")

#         for vid in all_vids:
#             neighbors = edges.get(vid, set())
#             neighbor_indices = sorted(vid_to_index[nbr] for nbr in neighbors if nbr in vid_to_index)
#             f.write(" ".join(map(str, neighbor_indices)) + "\n")

#     print(f"graph.txt written to {GRAPH_OUTPUT_FILE} with {num_nodes} nodes and {num_edges} edges")

# if __name__ == "__main__":
#     main()
