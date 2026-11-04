import asyncio
import logging


class BullyElection:
    def __init__(self, node_id: int, peers: dict, network: object):
        self.node_id = node_id
        self.peers = peers
        self.net = network
        self.leader_id = max(list(peers.keys()) + [node_id])
        self.is_electing = False
        self.received_answer = False

    def is_leader(self) -> bool:
        return self.leader_id == self.node_id

    async def start_election(self):
        if self.is_electing:
            return

        self.is_electing = True
        self.received_answer = False
        higher_peers = {
            pid: addr for pid, addr in self.peers.items() if pid > self.node_id
        }

        if not higher_peers:
            await self._announce_coordination()
            return

        for pid, (host, port) in higher_peers.items():
            asyncio.create_task(
                self.net.send_message(
                    host, port, {"type": "ELECTION", "sender_id": self.node_id}
                )
            )

        await asyncio.sleep(2.0)

        if not self.received_answer:
            await self._announce_coordination()

    async def handle_election_msg(self, sender_id: int):
        if self.node_id > sender_id:
            if sender_id in self.peers:
                host, port = self.peers[sender_id]
                asyncio.create_task(
                    self.net.send_message(
                        host, port, {"type": "ANSWER", "sender_id": self.node_id}
                    )
                )
            if not self.is_electing:
                asyncio.create_task(self.start_election())

    async def handle_answer_msg(self):
        self.received_answer = True

    async def handle_coordinator_msg(
        self, new_leader_id: int, heartbeat_detector: object
    ):
        self.leader_id = new_leader_id
        self.is_electing = False
        heartbeat_detector.update_heartbeat_timestamp()
        logging.info(
            f"Node {self.node_id}: Novo líder reconhecido -> Node {self.leader_id}"
        )

    async def _announce_coordination(self):
        self.leader_id = self.node_id
        self.is_electing = False
        logging.info(
            f"Node {self.node_id}: >>> VENCEU A ELEIÇÃO E ASSUMIU COMO LÍDER <<<"
        )
        for pid, (host, port) in self.peers.items():
            asyncio.create_task(
                self.net.send_message(
                    host, port, {"type": "COORDINATOR", "sender_id": self.node_id}
                )
            )
