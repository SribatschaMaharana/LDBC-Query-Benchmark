#4
import os
import csv
from collections import defaultdict
from neo4j import GraphDatabase

AUTH = ("neo4j", "playground")
SOCIAL_NETWORK_DIR = "import/social_network"
EXPORT_VID_MAP_DIR = "partitioned_vids"
VID_SID_LOG = os.path.join(EXPORT_VID_MAP_DIR, "vid_sid_log.csv")

DB_URIS = {
    0: "neo4j://localhost:9750",
    1: "neo4j://localhost:9751"
}

# NODE_FILES = {
#     "organisation_0_0.csv": ("Organisation", ["type", "name", "url"]),
#     "place_0_0.csv": ("Place", ["name", "url", "type"]),
# }
NODE_FILES = {
    "person_0_0.csv": ("Person", [
        "firstName", "lastName", "gender", "birthday","creationDate", "locationIP", "browserUsed"
    ]),
    # "post_0_0.csv": ("Post", [
    #     "imageFile", "creationDate", "locationIP", "browserUsed","language", "content", "length"
    # ]),
    # "comment_0_0.csv": ("Comment", [
    #     "creationDate", "locationIP", "browserUsed","content", "length"
    # ]),
    "place_0_0.csv": ("Place", [
        "name", "url", "type"
    ]),
    "organisation_0_0.csv": ("Organisation", [
        "type", "name", "url"
    ]),
    "forum_0_0.csv": ("Forum", [
        "title", "creationDate"
    ]),
    "tag_0_0.csv": ("Tag", [
        "name", "url"
    ]),
    "tagclass_0_0.csv": ("TagClass", [
        "name", "url"
    ]),
}
# Maps original_id -> vid, vid -> sid
vid_maps = defaultdict(dict)
vid_to_sid = {}

def load_vid_maps():
    for filename, (label, _) in NODE_FILES.items():
        map_path = os.path.join(EXPORT_VID_MAP_DIR, f"{label.lower()}_vid_map.csv")
        with open(map_path, newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                vid_maps[label][int(row["original_id"])] = int(row["vid"])
        print(f"Loaded vid map for {label}")

def load_vid_sid_log():
    with open(VID_SID_LOG, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid_to_sid[int(row["vid"])] = int(row["sid"])
    print(f"Loaded sid mappings from METIS output")

def load_nodes_for(label, filename, fields):
    nodes = []
    filepath = None

    for subfolder in ["static", "dynamic"]:
        dir = os.path.join(SOCIAL_NETWORK_DIR, subfolder)
        file_path_candidate = os.path.join(dir, filename)
        if os.path.exists(file_path_candidate):
            filepath = file_path_candidate
            break

    if not filepath:
        print(f"{filename} not found.")
        return nodes

    print(f"Loading {label} nodes from {filepath}")
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            original_id = int(row["id"])
            vid = vid_maps[label].get(original_id)
            if vid is None:
                continue

            node_props = {"vid": vid}
            for field in fields:
                val = row.get(field)
                if val and field == "length":
                    val = int(val)
                node_props[field] = val
            nodes.append((vid, node_props))

    print(f"Loaded {len(nodes)} nodes for {label}")
    return nodes

def push_partitioned(label, nodes):
    server_batches = defaultdict(list)
    for vid, props in nodes:
        sid = vid_to_sid.get(vid)
        if sid is None:
            continue
        server_batches[sid].append(props)

    for sid, batch in server_batches.items():
        uri = DB_URIS.get(sid)
        if uri:
            push_to_db(uri, label, batch)
        else:
            print(f"No DB URI configured  {sid}")

def push_to_db(uri, label, nodes, batch_size=500):
    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            for i in range(0, len(nodes), batch_size):
                batch = nodes[i:i + batch_size]
                session.run(f"""
                    UNWIND $batch AS row
                    MERGE (n:{label} {{vid: row.vid}})
                    SET n += row
                """, {"batch": batch})
            print(f"Pushed {len(nodes)} nodes to {uri} ({label})")

def insert_proxy_node(uri):
    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            session.run("""
                MERGE (:ProxyUniversal {label: 'proxy', dummy: true})
            """)
        print(f"Inserted proxy node in {uri}")

if __name__ == "__main__":
    load_vid_maps()
    load_vid_sid_log()

    for filename, (label, fields) in NODE_FILES.items():
        nodes = load_nodes_for(label, filename, fields)
        if nodes:
            push_partitioned(label, nodes)

    for uri in DB_URIS.values():
        insert_proxy_node(uri)
