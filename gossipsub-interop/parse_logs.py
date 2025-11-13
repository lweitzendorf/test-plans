import glob
import os
import sys
import json
import shutil
import heapq
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Self

import pydot
import numpy as np
import matplotlib.pyplot as plt

from dateutil import parser


peer_ids: dict[int, str] = {}
meshes: dict[str, dict[str, set[str]]] = {}
bytes_sent_payload: dict[str, int] = {}
bytes_sent_control: dict[str, int] = {}

msg_send_times: dict[int, str] = {}
msg_delivery_times: dict[int, dict[str, str]] = defaultdict(dict)

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
            
class LogEntry:
    def __init__(self, node_id: int, log: dict) -> None:
        self.node = node_id
        self.time = parser.isoparse(log["time"])
        self.log = log
        
    def get(self, arg, default=None) -> Optional[str | int]:
        return self.log.get(arg, default)
        
    def __getitem__(self, arg) -> str | int:
        return self.log[arg]
    
    def __lt__(self, other: Self) -> bool:
        return self.time < other.time


def parse_log_files(root_dir: str) -> Iterable[LogEntry]:
    root_dir = os.path.join(root_dir, "hosts")
    
    def parse_single_file(_node_idx: int, _file_path: str) -> Iterable[LogEntry]:
        with open(_file_path, 'r') as f:
            for log_line in f:
                yield LogEntry(_node_idx, json.loads(log_line))
                
        # yield LogEntry(_node_idx, {"msg": "Shutdown", "time": logs[-1]["time"]})
        
    heads = []            

    for node_dir in os.listdir(root_dir):
        node_idx = int(node_dir.removeprefix("node"))
            
        for log_file in os.listdir(os.path.join(root_dir, node_dir)):
            if not log_file.endswith(".stdout"):
                continue
            
            file_path = os.path.join(root_dir, node_dir, log_file)
            iterator = parse_single_file(node_idx, file_path)
            if head := next(iterator, None):
                heads.append((head, iterator))
                
    heapq.heapify(heads)

    while heads:
        head, iterator = heapq.heappop(heads)
        yield head
        if head := next(iterator, None):
            heapq.heappush(heads, (head, iterator))


def save_snapshot(graph_dir: str, elapsed_time: timedelta) -> None:
    print(f"Taking snapshot @ {elapsed_time} ...")
    
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
        file_name = f"{str(elapsed_time).replace(':', '-')}.dot"
        file_path = os.path.join(topic_dir, file_name)

        with open(file_path, 'w') as f:
            f.write(graph.to_string())

        file_path = os.path.join(topic_dir, f"{elapsed_time}.png")

        degrees = [len(to_ids) for to_ids in mesh.values()]
        counts, bins = np.histogram(degrees)
        plt.title(f"Degree distribution after {elapsed_time}")
        plt.hist(bins[:-1], bins, weights=counts)
        plt.savefig(file_path)
        plt.clf()


    with open(os.path.join(graph_dir, f"{elapsed_time}.json"), 'w') as f:
        json.dump({
            "bytes_payload": bytes_sent_payload,
            "bytes_control": bytes_sent_control,
            "message_sends": msg_send_times,
            "message_deliveries": msg_delivery_times
        }, f, indent=4)


def register_sent_bytes(node_id: str, num_bytes: int, is_payload: bool) -> None:
    bytes_sent = bytes_sent_payload if is_payload else bytes_sent_control
    bytes_sent[node_id] = bytes_sent.get(node_id, 0) + num_bytes

def register_message_send(message_id: int, node_id: str, timestamp: str) -> None:
    if message_id not in msg_send_times:
        msg_send_times[message_id] = timestamp
    register_message_delivery(message_id, node_id, timestamp)

def register_message_delivery(message_id: int, node_id: str, timestamp: str) -> None:
    if (node_id not in msg_delivery_times[message_id]):
        msg_delivery_times[message_id][node_id] = timestamp

def process_logs(test_dir: str, logs: Iterable[LogEntry]) -> str:
    snapshot_duration = timedelta(minutes=5)
    genesis_time = datetime(2000, 1, 1, tzinfo=timezone.utc)
    next_snapshot = snapshot_duration

    data_dir = os.path.join(test_dir, "data")
    print(f"Creating {data_dir} ...")
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir)
    
    take_snapshot = True
    snapshot_counter = 0

    for log in logs:
        node_idx = log.node

        match log["msg"]:
            case "PeerID":
                peer_ids[node_idx] = log["id"]
            case "Sent Graft":
                add_connection(log.get("topic", "default"), peer_ids[node_idx], log["to"])
                register_sent_bytes(peer_ids[node_idx], log["size"], False)
                take_snapshot = True
            case "Received Graft":
                add_connection(log.get("topic", "default"), peer_ids[node_idx], log["from"])
                take_snapshot = True
            case "Added Peer":
                add_connection("default", peer_ids[node_idx], log["id"])
                take_snapshot = True
            case "Removed Peer":
                remove_connection("default", peer_ids[node_idx], log["id"])
                take_snapshot = True
            case "Sent Prune":
                remove_connection(log.get("topic", "default"), peer_ids[node_idx], log["to"])
                register_sent_bytes(peer_ids[node_idx], log["size"], False)
                take_snapshot = True
            case "Received Prune":
                remove_connection(log.get("topic", "default"), peer_ids[node_idx], log["from"])
                take_snapshot = True
            # case "Shutdown":
            #    remove_node(peer_ids[node_idx])
            # case "Publish":
            #    register_message_send(log["id"], peer_ids[node_idx], log["time"])
            #    register_sent_bytes(peer_ids[node_idx], log["size"], True)
            case "Sent Message":
                register_message_send(log["id"], peer_ids[node_idx], log["time"])
                register_sent_bytes(peer_ids[node_idx], log["size"], True)
            case "Received Message":
                register_message_delivery(log["id"], peer_ids[node_idx], log["time"])
            case msg if msg.startswith("Sent"):
                register_sent_bytes(peer_ids[node_idx], log["size"], False)
                
        elapsed_time = log.time - genesis_time
        if take_snapshot:
            if (snapshot_counter % 25 == 0):
                save_snapshot(data_dir, elapsed_time)
            snapshot_counter += 1
            take_snapshot = False
        elif (elapsed_time >= next_snapshot):
            save_snapshot(data_dir, elapsed_time)
            next_snapshot += snapshot_duration
        

    # end_time = datetime.fromisoformat(logs[-1]["time"])
    # save_snapshot(data_dir, end_time - genesis_time)

    return data_dir


def plot_total_network_traffic(plots_dir: str, json_data: list[tuple[int, dict]]) -> None:
    x = [0]
    y = [{"optimal": 0, "payload": 0, "control": 0}]

    num_nodes = len(json_data[-1][1]["bytes_payload"].keys())

    for total_minutes, data in json_data:
        x.append(total_minutes)
        y.append({
            "optimal": (num_nodes - 1) * total_minutes * 5 * 1024,
            "payload": sum(data["bytes_payload"].values()),
            "control": sum(data["bytes_control"].values()),
        })

    plt.xlabel("Time (minutes)")
    plt.ylabel("Traffic multiple")
    plt.title("Multiple of Optimal Network Traffic")

    # conv_factor = 1_000_000

    # plt.plot(x, [e["payload"] // conv_factor for e in y], label="payload")
    # plt.plot(x, [e["control"] // conv_factor for e in y], label="control")
    # plt.plot(x, [e["optimal"] // conv_factor for e in y], label="payload_optimal")
    # plt.legend()
    
    x_new = []
    y_new = []
    
    for i in range(1, len(y)):
        delta_payload = y[i]["payload"] - y[i-1]["payload"]
        delta_optimal = y[i]["optimal"] - y[i-1]["optimal"]
        
        if (delta_optimal > 0) and (delta_payload > 0):
            x_new.append(x[i])
            y_new.append(delta_payload / delta_optimal)
            
    
    # plt.ylim(bottom=1)
    plt.plot(x_new, y_new)

    plt.savefig(os.path.join(plots_dir, "network_traffic.png"))
    plt.clf()


def plot_payload_traffic_by_node(plots_dir: str, json_data: list[tuple[int, dict]]) -> None:
    node_ids = set(json_data[-1][1]["bytes_payload"].keys())

    x = [0]
    y = [{node_id: 0 for node_id in node_ids}]

    for total_minutes, data in json_data:
        x.append(total_minutes)
        y.append({node_id: data["bytes_payload"].get(node_id, 0) for node_id in node_ids})

    plt.xlabel("Time (minutes)")
    plt.ylabel("Traffic (MB)")
    plt.title("Cumulative Payload Traffic by Node")

    conv_factor = 1_000_000

    for node_id in node_ids:
        plt.plot(x, [e[node_id] // conv_factor for e in y], label=node_id)

    # plt.legend()
    plt.savefig(os.path.join(plots_dir, "payload_traffic_by_node.png"))
    plt.clf()


def plot_message_delivery_times(plots_dir: str, json_data: list[tuple[int, dict]]) -> None:
    msg_send_data = json_data[-1][1]["message_sends"]
    msg_delivery_data = json_data[-1][1]["message_deliveries"]

    plt.xlabel("Message ID")
    plt.ylabel("Time (milliseconds)")
    plt.title("Message Delivery Latency")

    x = [int(msg_id) for msg_id in msg_delivery_data.keys() if (int(msg_id) % 100 == 0)]
    y = []

    conv_factor = 1000

    for i, msg_id in enumerate(x):
        send_ts = parser.isoparse(msg_send_data[str(msg_id)])
        delivery_times = [parser.isoparse(ts) for ts in msg_delivery_data[str(msg_id)].values()]
        delivery_times.sort()
        delivery_latencies = [ts - send_ts for ts in delivery_times[1:]]        
        y.append([t.total_seconds() * conv_factor for t in delivery_latencies])
        plt.scatter(i + np.random.normal(scale=0.1, size=len(y[-1])), y[-1], s=3)

    plt.boxplot(y)
    plt.xticks(list(range(len(x))), list(map(str, x)))

    plt.savefig(os.path.join(plots_dir, "message_latency.png"))
    plt.clf()


def generate_plots(test_dir: str, data_dir: str) -> str:
    plots_dir = os.path.join(test_dir, "plots")
    os.makedirs(plots_dir, exist_ok=True)

    json_data = {}

    for json_path in glob.glob(os.path.join(data_dir, "*.json")):
        file_name = os.path.basename(json_path)

        hours = int(file_name.split(":")[0])
        minutes = int(file_name.split(":")[1])
        total_minutes = 60 * hours + minutes

        with open(json_path, "r") as f:
            json_data[total_minutes] = json.load(f)

    json_data = sorted(json_data.items())

    plot_total_network_traffic(plots_dir, json_data)
    plot_payload_traffic_by_node(plots_dir, json_data)
    plot_message_delivery_times(plots_dir, json_data)

    return plots_dir


def main():
    test_dir = sys.argv[1]

    print("Parsing log files...")
    logs = parse_log_files(test_dir)

    print("Processing logs...")
    data_dir = process_logs(test_dir, logs)

    # data_dir = os.path.join(test_dir, "data")
    print("Generating graphs...")
    generate_plots(test_dir, data_dir)
    
    print("Done.")

        
if __name__ == "__main__":
    main()
