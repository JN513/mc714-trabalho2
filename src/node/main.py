import os
import asyncio
import logging
from network import NetworkManager
from alg1_bully import BullyElection
from alg2_heartbeat import HeartbeatDetector

NODE_ID = int(os.getenv("NODE_ID", "1"))
PORT = int(os.getenv("PORT", "5000"))
PEERS_RAW = os.getenv("PEERS", "")

PEERS = {}
if PEERS_RAW:
    for item in PEERS_RAW.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) == 3:
            # Formato explícito: ID:HOST:PORT (ex: 2:node2:5000)
            PEERS[int(parts[0])] = (parts[1], int(parts[2]))
        elif len(parts) == 2:
            # Formato reduzido: HOST:PORT (ex: node2:5000) -> extrai ID do nome do host
            host, port = parts[0], int(parts[1])
            node_id = int("".join(filter(str.isdigit, host)))
            PEERS[node_id] = (host, port)

hb_detector = None
bully = None


async def route_message(msg: dict):
    msg_type = msg.get("type")
    sender_id = msg.get("sender_id")

    if msg_type == "HEARTBEAT":
        if sender_id == bully.leader_id:
            hb_detector.update_heartbeat_timestamp()
    elif msg_type == "ELECTION":
        await bully.handle_election_msg(sender_id)
    elif msg_type == "ANSWER":
        await bully.handle_answer_msg()
    elif msg_type == "COORDINATOR":
        await bully.handle_coordinator_msg(sender_id, hb_detector)


async def main():
    global hb_detector, bully

    net = NetworkManager(NODE_ID, PORT, route_message)
    await net.start_server()

    bully = BullyElection(NODE_ID, PEERS, net)
    hb_detector = HeartbeatDetector(NODE_ID, net)

    await asyncio.gather(
        net.server.serve_forever(),
        hb_detector.run_sender(bully.is_leader, PEERS),
        hb_detector.run_checker(
            bully.is_leader, lambda: bully.is_electing, bully.start_election
        ),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
