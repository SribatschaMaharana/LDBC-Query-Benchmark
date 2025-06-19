import csv
from neo4j import GraphDatabase
import os

URI = "neo4j://localhost:7687"
AUTH = ("neo4j", "playground")
BATCH_SIZE = 500

def load_vid_map(path):
    id_to_vid = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_to_vid[int(row["original_id"])] = int(row["vid"])
    return id_to_vid

def load_relationships(csv_path, from_vid_map, to_vid_map):
    relationships = []
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            try:
                from_id = int(row[list(row.keys())[0]])
                to_id = int(row[list(row.keys())[1]])

                from_vid = from_vid_map[from_id]
                to_vid = to_vid_map[to_id]

                # relationship properties
                props = {}
                for key in list(row.keys())[2:]:
                    val = row[key]
                    if val.isdigit():
                        val = int(val)
                    props[key] = val

                relationships.append((from_vid, to_vid, props))
            except KeyError as e:
                print(f"Missing VID for: {e}")
                continue
    return relationships

def push_relationships_to_neo4j(relationships, from_label, to_label, rel_type):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            for i in range(0, len(relationships), BATCH_SIZE):
                batch = relationships[i:i + BATCH_SIZE]
                session.execute_write(create_batch, batch, from_label, to_label, rel_type)
                print(f"Pushed batch {i // BATCH_SIZE + 1} / {len(relationships) // BATCH_SIZE + 1}")
    print(f"Pushed a total of {len(relationships)} {rel_type} relationships.")



def create_batch(tx, rels, from_label, to_label, rel_type):
    tx.run(f"""
        UNWIND $rels AS row
        MATCH (a:{from_label} {{vid: row.from}})
        MATCH (b:{to_label} {{vid: row.to}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += row.props
    """, rels=[
        {"from": a, "to": b, "props": props} for a, b, props in rels
    ])

if __name__ == "__main__":
    #Edit here:
    CSV_PATH = "import/social_network/static/organisation_isLocatedIn_place_0_0.csv"
    FROM_LABEL = "Organisation"
    TO_LABEL = "Place"
    REL_TYPE = "isLocatedIn"
    FROM_VID_MAP_PATH = "social_network/id_to_vid_maps/organisation_vid_map.csv"
    TO_VID_MAP_PATH = "social_network/id_to_vid_maps/place_vid_map.csv"  

    from_vids = load_vid_map(FROM_VID_MAP_PATH)
    to_vids = load_vid_map(TO_VID_MAP_PATH)
    rels = load_relationships(CSV_PATH, from_vids, to_vids)

    print(f"Loaded {len(rels)} relationships.")
    push_relationships_to_neo4j(rels, FROM_LABEL, TO_LABEL, REL_TYPE)
