import os
import csv
from collections import defaultdict
from neo4j import GraphDatabase

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "playground")
SOCIAL_NETWORK_DIR = "import/social_network" 

# "nodes" and their corresponding labels + fields
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

# Global counter and maps
vid_counter = 1
id_to_vid_map = defaultdict(dict) 

def assign_vid(label, original_id):
    global vid_counter
    vid = vid_counter
    vid_counter += 1
    id_to_vid_map[label][original_id] = vid
    return vid


def export_vid_maps(output_dir="social_network/id_to_vid_maps"):
    os.makedirs(output_dir, exist_ok=True)
    for label, mapping in id_to_vid_map.items():
        filename = f"{label.lower()}_vid_map.csv"
        filepath = os.path.join(output_dir, filename)
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["original_id", "vid"])
            for original_id, vid in mapping.items():
                writer.writerow([original_id, vid])
        print(f"Exported {label} vid map to {filepath}")


def load_all_nodes():
    nodes_to_create = []

    for subfolder in ["static", "dynamic"]:
        dir = os.path.join(SOCIAL_NETWORK_DIR, subfolder)
        for filename in os.listdir(dir):
            if filename not in NODE_FILES:
                continue

            label, fields = NODE_FILES[filename]
            filepath = os.path.join(dir, filename)
            print(f"Loadin {label} nodes from {filename}")

            with open(filepath, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter='|')
                for row in reader:
                    original_id = int(row["id"])
                    vid = assign_vid(label, original_id)

                    node_props = {"vid": vid}
                    for field in fields:
                        val = row.get(field)
                        if val and field in ["length"]:  # convert numeric
                            val = int(val)
                        node_props[field] = val
                    nodes_to_create.append((label, node_props))

    return nodes_to_create

# def push_nodes_to_neo4j(data):
#     with GraphDatabase.driver(URI, auth=AUTH) as driver:
#         with driver.session(database="neo4j") as session:
#             for label, props in data:
#                 session.run(f"""
#                     MERGE (n:{label} {{vid: $vid}})
#                     SET {', '.join([f"n.{k} = ${k}" for k in props if k != 'vid'])}
#                 """, props)
#                 print(f"created node {label}")
#     print(f"Pushed {len(data)} nodes to Neo4j.")


def push_nodes_to_neo4j_batched(data, batch_size=500):
    grouped = defaultdict(list)
    for label, props in data:
        grouped[label].append(props)

    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            for label, nodes in grouped.items():
                print(f"Pushing {len(nodes)} {label} nodes in batches")
                for i in range(0, len(nodes), batch_size):
                    batch = nodes[i:i+batch_size]
                    session.run(f"""
                        UNWIND $batch AS row
                        MERGE (n:{label} {{vid: row.vid}})
                        SET n += row
                    """, {"batch": batch})
                print(f"Finished pushing {label}.")
    print(f"Pushed {len(data)} nodes to Neo4j.")



if __name__ == "__main__":
    all_nodes = load_all_nodes()
    export_vid_maps()
    push_nodes_to_neo4j_batched(all_nodes)