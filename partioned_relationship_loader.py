import os
import csv
from collections import defaultdict
from neo4j import GraphDatabase


PARTITION_DIR = "partitioned_vids"
BATCH_SIZE = 500

DB_URIS = {
    0: "neo4j://localhost:9750",
    1: "neo4j://localhost:9751"
}
AUTH = ("neo4j", "playground")


def load_vid_map(path):
    id_to_vid = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            id_to_vid[int(row["original_id"])] = int(row["vid"])
    return id_to_vid

def load_vid_sid_map(path=os.path.join(PARTITION_DIR, "vid_sid_log.csv")):
    vid_to_sid = {}
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid = int(row["vid"])
            sid = int(row["sid"])
            vid_to_sid[vid] = sid
    return vid_to_sid

def load_relationships_partitioned(csv_path, from_vid_map, to_vid_map, vid_to_sid):
    same_sid_batches = defaultdict(list)
    cross_sid  = defaultdict(list)

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            try:
                from_id = int(row[list(row.keys())[0]])
                to_id = int(row[list(row.keys())[1]])

                from_vid = from_vid_map[from_id]
                to_vid = to_vid_map[to_id]

                from_sid = vid_to_sid[from_vid]
                to_sid = vid_to_sid[to_vid]

                props = {}
                for key in list(row.keys())[2:]:
                    val = row[key]
                    if val.isdigit():
                        val = int(val)
                    props[key] = val

                if from_sid == to_sid:
                    same_sid_batches[from_sid].append((from_vid, to_vid, props))
                else:
                    #cross_sid.append((from_vid, to_vid, props))
                    props.update({
                        "proxy": True,
                        "target_vid": to_vid,
                        "target_sid": to_sid
                    })
                    cross_sid[from_sid].append((from_vid, props))

            except KeyError as e:
                print(f"Missing VID or SID for: {e}")
                continue

    return same_sid_batches, cross_sid

# push to uri 

def push_relationships_to_neo4j(uri, relationships, from_label, to_label, rel_type):
    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            for i in range(0, len(relationships), BATCH_SIZE):
                batch = relationships[i:i + BATCH_SIZE]
                session.execute_write(create_batch, batch, from_label, to_label, rel_type)
                print(f"SID batch pushed: {i // BATCH_SIZE + 1}")

#same as before
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

def push_proxy_relationships(uri, relationships, from_label, rel_type):
    with GraphDatabase.driver(uri, auth=AUTH) as driver:
        with driver.session(database="neo4j") as session:
            session.execute_write(create_proxy_batch, relationships, from_label, rel_type)

def create_proxy_batch(tx, rels, from_label, rel_type):
    tx.run(f"""
        UNWIND $rels AS row
        MATCH (a:{from_label} {{vid: row.from}})
        MATCH (b:ProxyUniversal {{label: 'proxy'}})
        MERGE (a)-[r:{rel_type}]->(b)
        SET r += row.props
    """, rels=[
        {"from": a, "props": props} for a, props in rels
    ])

if __name__ == "__main__":
    # edit this
    CSV_PATH = "import/social_network/static/organisation_isLocatedIn_place_0_0.csv"
    FROM_LABEL = "Organisation"
    TO_LABEL = "Place"
    REL_TYPE = "isLocatedIn"

    FROM_VID_MAP_PATH = os.path.join(PARTITION_DIR, "organisation_vid_map.csv")
    TO_VID_MAP_PATH = os.path.join(PARTITION_DIR, "place_vid_map.csv")

    # load vid maps and partition map
    from_vids = load_vid_map(FROM_VID_MAP_PATH)
    to_vids = load_vid_map(TO_VID_MAP_PATH)
    vid_to_sid = load_vid_sid_map()

    # partition relationships
    print(f"loading and partitioning relationships from {CSV_PATH}")
    same_sid_batches, cross_sid = load_relationships_partitioned(CSV_PATH, from_vids, to_vids, vid_to_sid)

    # same-SID relationships
    for sid, batch in same_sid_batches.items():
        uri = DB_URIS.get(sid)
        if not uri:
            print(f"no URI for sid={sid}")
            continue
        print(f"Pushing {len(batch)} relationships to SID {sid} ({uri})")
        push_relationships_to_neo4j(uri, batch, FROM_LABEL, TO_LABEL, REL_TYPE)

    print(f"finished pushing same-instance relationships.")
    print(f"skipped {len(cross_sid)} cross-instance relationships.")
    for sid, batch in cross_sid.items():
        uri = DB_URIS.get(sid)
        if not uri:
            print(f"no URI for sid={sid}")
            continue
        print(f"Pushing {len(batch)} proxy relationships to SID {sid} ({uri})")
        push_proxy_relationships(uri, batch, FROM_LABEL, REL_TYPE)

    cross_path = os.path.join(PARTITION_DIR, f"{REL_TYPE}_cross_instance.csv")
    with open(cross_path, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["from_vid", "to_vid", "props"])
        for sid_batch in cross_sid.values():
            for a, props in sid_batch:
                writer.writerow([a, props["target_vid"], str(props)])

    print(f"Cross-instance relationships saved to: {cross_path}")
