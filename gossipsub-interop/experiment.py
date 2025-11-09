import math
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
import random
from typing import List, Dict, Set

from script_instruction import GossipSubParams, ScriptInstruction, NodeID
import script_instruction


@dataclass
class Binary:
    path: str
    percent_of_nodes: int


@dataclass
class ExperimentParams:
    script: List[ScriptInstruction] = field(default_factory=list)


def spread_heartbeat_delay(node_count: int, template_gs_params: GossipSubParams) -> List[ScriptInstruction]:
    instructions = []
    initial_delay = timedelta(seconds=0.1)
    for i in range(node_count):
        initial_delay += timedelta(milliseconds=0.100)
        gs_params = template_gs_params.model_copy()
        # The value is in nanoseconds
        gs_params.HeartbeatInitialDelay = initial_delay.microseconds * 1_000
        instructions.append(
            script_instruction.IfNodeIDEquals(
                nodeID=i,
                instruction=script_instruction.InitGossipSub(
                    gossipSubParams=gs_params)
            )
        )
    return instructions


def scenario(scenario_name: str, node_count: int, disable_gossip: bool) -> ExperimentParams:
    instructions: List[ScriptInstruction] = []
    match scenario_name:
        case "subnet-blob-msg":
            gs_params = GossipSubParams()
            if disable_gossip:
                gs_params.Dlazy = 0
                gs_params.GossipFactor = 0
            instructions.extend(spread_heartbeat_delay(
                node_count, gs_params))

            topic = "a-subnet"
            blob_count = 48
            # According to data gathered by lighthouse, a column takes around
            # 5ms.
            instructions.append(
                script_instruction.SetTopicValidationDelay(
                    topicID=topic, delaySeconds=0.005)
            )
            number_of_conns_per_node = 20
            if number_of_conns_per_node >= node_count:
                number_of_conns_per_node = node_count - 1
            instructions.extend(
                random_network_mesh(node_count, number_of_conns_per_node)
            )
            message_size = 2 * 1024 * blob_count
            num_messages = 16
            instructions.append(
                script_instruction.SubscribeToTopic(topicID=topic))
            instructions.extend(
                random_publish_every_12s(
                    node_count, num_messages, message_size, [topic])
            )
        case "simple-fanout":
            gs_params = GossipSubParams()
            if disable_gossip:
                gs_params.Dlazy = 0
                gs_params.GossipFactor = 0
            instructions.extend(spread_heartbeat_delay(
                node_count, gs_params))
            topic_a = "topic-a"
            topic_b = "topic-b"
            number_of_conns_per_node = 20
            if number_of_conns_per_node >= node_count:
                number_of_conns_per_node = node_count - 1
            instructions.extend(
                random_network_mesh(node_count, number_of_conns_per_node)
            )

            # Half nodes will subscribe to topic-a, the other half subscribe to
            # topic-b
            for i in range(node_count):
                if i % 2 == 0:
                    instructions.append(
                        script_instruction.IfNodeIDEquals(
                            nodeID=i,
                            instruction=script_instruction.SubscribeToTopic(topicID=topic_a)),
                    )
                else:
                    instructions.append(
                        script_instruction.IfNodeIDEquals(
                            nodeID=i,
                            instruction=script_instruction.SubscribeToTopic(topicID=topic_b)),
                    )

            num_messages = 16
            message_size = 1024

            # Every 12s a random node will publish to a random topic
            instructions.extend(
                random_publish_every_12s(
                    node_count, num_messages, message_size, [topic_a, topic_b]))

        case "longevity":
            gs_params = GossipSubParams()

            if disable_gossip:
                gs_params.Dlazy = 0
                gs_params.GossipFactor = 0
            #instructions.extend(spread_heartbeat_delay(node_count, gs_params))

            number_of_conns_per_node = 20
            if number_of_conns_per_node >= node_count:
                number_of_conns_per_node = node_count - 1
            instructions.extend(
                random_network_mesh(node_count, number_of_conns_per_node)
                # line_mesh(node_count)
                # isolated_cluster_mesh(node_count, number_of_conns_per_node, int(math.sqrt(node_count)))
                # cluster_bridge_mesh(node_count, number_of_conns_per_node, int(math.sqrt(node_count)))
                # star_mesh(node_count, number_of_conns_per_node)
            )

            topic = "topic-a"

            #instructions.append(
            #    script_instruction.SubscribeToTopic(topicID=topic))
            
            instructions.extend(random_publish_every_12s(
                node_count=node_count,
                num_messages=100,
                message_size=1024,
                topic_strs=[topic]
            ))
            
            """
            instructions.extend(
                random_publish_every_12s(
                    node_count=node_count,
                    num_messages=5,
                    message_size=1024,
                    topic_strs=[topic]
                )
            )
            instructions.extend(random_network_mesh(node_count, node_count // 2))
            instructions.extend(
                random_publish_every_12s(
                    node_count=node_count,
                    num_messages=95,
                    message_size=1024,
                    topic_strs=[topic]
                )
            )
            """

        case _:
            raise ValueError(f"Unknown scenario name: {scenario_name}")

    return ExperimentParams(script=instructions)


def composition(preset_name: str) -> List[Binary]:
    match preset_name:
        case "gossipsub":
            return [Binary("go-libp2p/gossipsub-bin", percent_of_nodes=100)]
        case "dog":
            return [Binary("libp2p-dog/target/debug/experiment", percent_of_nodes=100)]
    raise ValueError(f"Unknown preset name: {preset_name}")


def isolated_cluster_mesh(num_nodes: int, number_of_connections: int, num_clusters: int) -> List[ScriptInstruction]:
    instructions = []

    cluster_size = num_nodes // num_clusters
    if number_of_connections >= cluster_size:
        number_of_connections = cluster_size - 1

    for cluster_idx in range(num_clusters):
        idx_offset = cluster_idx * cluster_size
        for instruction in random_network_mesh(cluster_size, number_of_connections):
            instruction.nodeID += idx_offset
            for i in range(len(instruction.instruction.connectTo)):
                instruction.instruction.connectTo[i] += idx_offset
            instructions.append(instruction)

    return instructions


def cluster_bridge_mesh(num_nodes: int, number_of_connections: int, num_clusters: int) -> List[ScriptInstruction]:
    instructions = []

    cluster_size = num_nodes // num_clusters
    if number_of_connections >= cluster_size:
        number_of_connections = cluster_size - 1

    for cluster_idx in range(num_clusters):
        idx_offset = cluster_idx * cluster_size
        for instruction in random_network_mesh(cluster_size, number_of_connections - 1):
            instruction.nodeID += idx_offset
            for i in range(len(instruction.instruction.connectTo)):
                instruction.instruction.connectTo[i] += idx_offset

            connections = set(instruction.instruction.connectTo)
            while len(connections) == len(instruction.instruction.connectTo):
                new_node = random.randint(0, num_nodes - 1)
                if new_node not in connections:
                    instruction.instruction.connectTo.append(new_node)

            instructions.append(instruction)

    return instructions


def star_mesh(num_nodes: int, number_of_connections: int) -> List[ScriptInstruction]:
    num_stars = (num_nodes // number_of_connections) + 1

    instructions = random_network_mesh(num_stars, min(num_stars - 1, number_of_connections // 2))
    for periphery_id in range(num_stars, num_nodes):
        star_id = periphery_id % num_stars
        instructions.append(
            script_instruction.IfNodeIDEquals(
                nodeID=periphery_id,
                instruction=script_instruction.Connect(
                    connectTo=[star_id],
                ),
            )
        )

    return instructions


def line_mesh(num_nodes: int) -> List[ScriptInstruction]:
    instructions = []

    for node_id in range(num_nodes):
        instructions.append(
            script_instruction.IfNodeIDEquals(
                nodeID=node_id,
                instruction=script_instruction.Connect(
                    connectTo=[(node_id + 1) % num_nodes],
                ),
            )
        )

    return instructions


def random_network_mesh(
    node_count: int, number_of_connections: int
) -> List[ScriptInstruction]:
    connections: Dict[NodeID, Set[NodeID]] = defaultdict(set)
    connect_to: Dict[NodeID, List[NodeID]] = defaultdict(list)
    for node_id in range(node_count):
        while len(connections[node_id]) < number_of_connections:
            target = random.randint(0, node_count - 1)
            if (target == node_id) or (target in connections[node_id]):
                continue

            connections[node_id].add(target)
            connect_to[node_id].append(target)
            connections[target].add(node_id)


    instructions = []
    for node_id, node_connections in connect_to.items():
        instructions.append(
            script_instruction.IfNodeIDEquals(
                nodeID=node_id,
                instruction=script_instruction.Connect(
                    connectTo=list(node_connections),
                ),
            )
        )
    return instructions


def random_publish_every_12s(
    node_count: int, num_messages: int, message_size: int, topic_strs: List[str]
) -> List[ScriptInstruction]:
    instructions = []

    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_seconds = 120
    instructions.append(script_instruction.WaitUntil(
        elapsedSeconds=elapsed_seconds))

    for i in range(num_messages):
        random_node = random.randint(0, node_count - 1)
        topic_str = random.choice(topic_strs)
        instructions.append(
            script_instruction.IfNodeIDEquals(
                nodeID=random_node,
                instruction=script_instruction.Publish(
                    messageID=i,
                    topicID=topic_str,
                    messageSizeBytes=message_size,
                ),
            )
        )
        elapsed_seconds += 12  # Add 12 seconds for each subsequent message
        instructions.append(
            script_instruction.WaitUntil(elapsedSeconds=elapsed_seconds)
        )

    elapsed_seconds += 30  # wait a bit more to allow all messages to flush
    instructions.append(script_instruction.WaitUntil(
        elapsedSeconds=elapsed_seconds))

    return instructions


def all_publish_every_12s(
        node_count: int, num_messages: int, message_size: int, topic_strs: List[str]
) -> List[ScriptInstruction]:
    instructions = []

    # Start at 120 seconds (2 minutes) to allow for setup time
    elapsed_seconds = 120
    instructions.append(script_instruction.WaitUntil(elapsedSeconds=elapsed_seconds))

    message_id = 0

    for i in range(num_messages):
        for topic_str in topic_strs:
            for node in range(node_count):
                instructions.append(
                    script_instruction.IfNodeIDEquals(
                        nodeID=node,
                        instruction=script_instruction.Publish(
                            messageID=message_id,
                            topicID=topic_str,
                            messageSizeBytes=message_size,
                        )
                    ),
                )
                message_id += 1
        elapsed_seconds += 12  # Add 12 second for each subsequent message
        instructions.append(
            script_instruction.WaitUntil(elapsedSeconds=elapsed_seconds)
        )

    elapsed_seconds += 30  # wait a bit more to allow all messages to flush
    instructions.append(script_instruction.WaitUntil(
        elapsedSeconds=elapsed_seconds))

    return instructions
