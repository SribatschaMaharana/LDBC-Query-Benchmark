#1
import os
import csv
from collections import defaultdict

SOCIAL_NETWORK_DIR = "import/social_network"
EXPORT_VID_MAP_DIR = "partitioned_vids"
VID_COUNTER_FILE = "vid_counter.txt"

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

vid_counter = 1
id_to_vid_map = defaultdict(dict)

def load_vid_counter():
    if os.path.exists(VID_COUNTER_FILE):
        with open(VID_COUNTER_FILE) as f:
            return int(f.read().strip())
    return 1

def save_vid_counter():
    with open(VID_COUNTER_FILE, "w") as f:
        f.write(str(vid_counter))

def assign_vid(label, original_id):
    global vid_counter
    vid = vid_counter
    vid_counter += 1
    id_to_vid_map[label][original_id] = vid
    return vid

def export_vid_maps():
    os.makedirs(EXPORT_VID_MAP_DIR, exist_ok=True)
    for label, mapping in id_to_vid_map.items():
        outpath = os.path.join(EXPORT_VID_MAP_DIR, f"{label.lower()}_vid_map.csv")
        with open(outpath, "w", newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["original_id", "vid"])
            for original_id, vid in mapping.items():
                writer.writerow([original_id, vid])
        print(f"Exported VID map for {label} to {outpath}")

def load_nodes_for(label, filename):
    filepath = None
    for subfolder in ["static", "dynamic"]:
        dir = os.path.join(SOCIAL_NETWORK_DIR, subfolder)
        file_path_candidate = os.path.join(dir, filename)
        if os.path.exists(file_path_candidate):
            filepath = file_path_candidate
            break

    if not filepath:
        print(f"File {filename} not found.")
        return

    print(f"Reading IDs for {label} from {filepath}")
    with open(filepath, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='|')
        for row in reader:
            original_id = int(row["id"])
            assign_vid(label, original_id)

if __name__ == "__main__":
    vid_counter = load_vid_counter()
    for filename, (label, _) in NODE_FILES.items():
        load_nodes_for(label, filename)
    export_vid_maps()
    save_vid_counter()
