import os
import sys
import json
from operator import itemgetter
from datetime import datetime, timedelta, timezone

import pydot
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm


peer_ids: dict[int, str] = {}
meshes: dict[str, dict[str, set[str]]] = {}


def add_connection(topic: str, from_id: str, to_id: str) -> None:
    if topic not in meshes:
        meshes[topic] = {}

    mesh = meshes[topic]

    if from_id not in mesh:
        mesh[from_id] = set()

    mesh[from_id].add(to_id)


def remove_connection(topic: str, from_id: str, to_id: str) -> None:
    if not (mesh := meshes.get(topic)):
        return

    if not (peers := mesh.get(from_id)):
        return

    if to_id in peers:
        peers.remove(to_id)

def remove_node(node_id: str):
    for mesh in meshes.values():
        if node_id in mesh:
            mesh.pop(node_id)


def parse_log_files(root_dir: str) -> list[dict]:
    root_dir = os.path.join(root_dir, "hosts")

    logs = []

    for node_dir in tqdm(os.listdir(root_dir)):
        node_idx = int(node_dir.removeprefix("node"))
        
        for log_file in os.listdir(os.path.join(root_dir, node_dir)):
            if not log_file.endswith(".stdout"):
                continue

            with open(os.path.join(root_dir, node_dir, log_file), 'r') as f:
                for log_line in f.readlines():
                    log = {"node": node_idx} | json.loads(log_line)

                    if not any(kw in log["msg"] for kw in ["PeerID", "Prune", "Graft"]):
                        continue

                    logs.append(log)

            logs.append({"node": node_idx, "msg": "Shutdown", "time": logs[-1]["time"]})

    return logs


def save_snapshot(root_dir: str, elapsed_time: timedelta) -> None:
    graph_dir = os.path.join(root_dir, "graphs")
    print(f"Creating {graph_dir} ...")
    os.removedirs(graph_dir)
    os.makedirs(graph_dir)

    for topic, mesh in meshes.items():
        graph = pydot.Dot(topic, graph_type="digraph")

        for idx, peer_id in peer_ids.items():
            node = pydot.Node(peer_id, label=str(idx))
            graph.add_node(node)

        for from_id, to_ids in mesh.items():
            for to_id in to_ids:
                edge = pydot.Edge(from_id, to_id)
                graph.add_edge(edge)

        topic_dir = os.path.join(graph_dir, topic)
        os.makedirs(topic_dir, exist_ok=True)

        file_path = os.path.join(topic_dir, f"{elapsed_time}.dot")

        with open(file_path, 'w') as f:
            f.write(graph.to_string())

        file_path = os.path.join(topic_dir, f"{elapsed_time}.png")
        print(f"Writing {file_path} ...")

        degrees = [len(to_ids) for to_ids in mesh.values()]
        counts, bins = np.histogram(degrees)
        plt.title(f"Degree distribution after {elapsed_time}")
        plt.hist(bins[:-1], bins, weights=counts)
        plt.savefig(file_path)
        plt.clf()


def process_logs(directory: str, logs: list[dict]) -> None:
    snapshot_duration = timedelta(minutes=15)
    genesis_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
    next_snapshot = snapshot_duration

    logs.sort(key=itemgetter("time"))

    for log in logs:
        node_idx = log["node"]

        match log["msg"]:
            case "PeerID":
                peer_ids[node_idx] = log["id"]
            case "Sent Graft":
                add_connection(log["topic"], peer_ids[node_idx], log["to"])
            case "Received Graft":
                add_connection(log["topic"], peer_ids[node_idx], log["from"])
            case "Sent Prune":
                remove_connection(log["topic"], peer_ids[node_idx], log["to"])
            case "Received Prune":
                remove_connection(log["topic"], peer_ids[node_idx], log["from"])
            case "Shutdown":
                remove_node(peer_ids[node_idx])
            case _:
                pass

        elapsed_time = datetime.fromisoformat(log["time"]) - genesis_time
        print(f"{elapsed_time = }")
        if elapsed_time >= next_snapshot:
            save_snapshot(directory, elapsed_time)
            next_snapshot += snapshot_duration

    # end_time = datetime.fromisoformat(logs[-1]["time"])
    # save_snapshot(directory, end_time - genesis_time)

                    
def main():
    test_dir = sys.argv[1]

    print("Parsing log files...")
    logs = parse_log_files(test_dir)

    print("Processing logs...")
    process_logs(test_dir, logs)

        
if __name__ == "__main__":
    main()
