import os
import asyncio
import logging
from network import NetworkManager
from alg1_bully import BullyElection
from alg2_heartbeat import HeartbeatDetector
from alg3_2pc import TwoPhaseCommit, KeyValueStore

NODE_ID = int(os.getenv("NODE_ID", "1"))
PORT = int(os.getenv("PORT", "5000"))
PEERS_RAW = os.getenv("PEERS", "")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "1.0"))
HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "3.0"))
ELECTION_TIMEOUT = float(os.getenv("ELECTION_TIMEOUT", "2.0"))
BOOTSTRAP_DELAY = float(os.getenv("BOOTSTRAP_DELAY", "2.0"))
VOTE_TIMEOUT = float(os.getenv("VOTE_TIMEOUT", "2.0"))
DECISION_TIMEOUT = float(os.getenv("DECISION_TIMEOUT", "3.0"))
CRASH_AFTER_PREPARE = os.getenv("CRASH_AFTER_PREPARE", "0") == "1"
PARTICIPANT_VOTE_NO = os.getenv("PARTICIPANT_VOTE_NO", "0") == "1"

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
            # Formato reduzido: HOST:PORT (ex: node2:5000) -> extrai ID do hostname
            host, port = parts[0], int(parts[1])
            node_id = int("".join(filter(str.isdigit, host)))
            PEERS[node_id] = (host, port)

hb_detector = None
bully = None
tpc = None


async def route_message(msg: dict):
    msg_type = msg.get("type")
    sender_id = msg.get("sender_id")

    if not msg_type:
        return

    # --- Eleição (Bully) e Detecção de Falhas (Heartbeat) ---
    if msg_type == "HEARTBEAT":
        if sender_id is not None and sender_id < NODE_ID:
            # Nó inferior enviando heartbeat indevidamente: desafia ou reanuncia
            if bully.is_leader():
                asyncio.create_task(bully._announce_coordination())
            elif not bully.is_electing:
                logging.info(
                    f"Node {NODE_ID}: Recebeu heartbeat de nó inferior ({sender_id}). Disparando eleição."
                )
                asyncio.create_task(bully.start_election())
        elif sender_id == bully.leader_id:
            hb_detector.update_heartbeat_timestamp()
        elif sender_id is not None and sender_id > (bully.leader_id or 0):
            # Líder legítimo com ID superior reconhecido
            bully.abdicate_if_higher_leader(sender_id)
            await bully.handle_coordinator_msg(sender_id, hb_detector)

    elif msg_type == "ELECTION":
        if sender_id is not None:
            await bully.handle_election_msg(sender_id)

    elif msg_type == "ANSWER":
        await bully.handle_answer_msg()

    elif msg_type == "COORDINATOR":
        if sender_id is not None:
            bully.abdicate_if_higher_leader(sender_id)
            await bully.handle_coordinator_msg(sender_id, hb_detector)

    # --- Compromisso Atômico (Two-Phase Commit / 2PC - Bônus) ---
    elif msg_type == "TXN":
        await tpc.handle_client_txn(msg.get("ops", []), sender_id)
    elif msg_type == "PREPARE":
        await tpc.handle_prepare(msg["txn_id"], msg.get("ops", []), sender_id)
    elif msg_type == "VOTE":
        await tpc.handle_vote(msg["txn_id"], msg["vote"], sender_id)
    elif msg_type == "DECISION":
        await tpc.handle_decision(msg["txn_id"], msg["decision"])
    elif msg_type == "QUERY":
        await tpc.handle_query(msg["txn_id"], sender_id)
    elif msg_type == "STATE":
        await tpc.handle_state(msg["txn_id"], msg["state"], sender_id)
    elif msg_type == "SNAPSHOT":
        await tpc.handle_snapshot()


async def main():
    global hb_detector, bully, tpc

    net = NetworkManager(NODE_ID, PORT, route_message)
    await net.start_server()

    bully = BullyElection(NODE_ID, PEERS, net, election_timeout=ELECTION_TIMEOUT)
    hb_detector = HeartbeatDetector(
        NODE_ID, net, interval=HEARTBEAT_INTERVAL, timeout=HEARTBEAT_TIMEOUT
    )
    tpc = TwoPhaseCommit(
        NODE_ID,
        PEERS,
        net,
        KeyValueStore(),
        get_leader_id=lambda: bully.leader_id,
        vote_timeout=VOTE_TIMEOUT,
        decision_timeout=DECISION_TIMEOUT,
        crash_after_prepare=CRASH_AFTER_PREPARE,
        participant_vote_no=PARTICIPANT_VOTE_NO,
    )

    await asyncio.gather(
        net.server.serve_forever(),
        hb_detector.run_sender(bully.is_leader, PEERS),
        hb_detector.run_checker(
            bully.is_leader, lambda: bully.is_electing, bully.start_election
        ),
        # Inicialização e recuperação ordenada (crash-recovery)
        bully.bootstrap(BOOTSTRAP_DELAY),
        # Monitor de terminação cooperativo do 2PC
        tpc.run_termination_monitor(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
