"""Testes de integração dos algoritmos usando uma rede em memória e mocks.

A rede em memória substitui o TCP por entrega assíncrona direta entre os
handlers dos nós, permitindo verificar as propriedades formais de correção
(segurança, vivacidade, recuperação e complexidade) sem depender de contêineres.
"""

import sys
import os
import unittest
import asyncio
from unittest.mock import AsyncMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/node")))

from alg1_bully import BullyElection
from alg2_heartbeat import HeartbeatDetector


class InMemoryBus:
    """Barramento de mensagens assíncrono entre nós lógicos."""

    def __init__(self):
        self.handlers = {}
        self.offline = set()
        self.messages = 0

    def register(self, node_id, handler):
        self.handlers[node_id] = handler

    def crash(self, node_id):
        self.offline.add(node_id)

    def recover(self, node_id):
        self.offline.discard(node_id)

    async def send_message(self, host, port, payload):
        target = int("".join(filter(str.isdigit, host)))
        if target in self.offline or target not in self.handlers:
            return False
        self.messages += 1
        await self.handlers[target](payload)
        return True


class HeartbeatStub:
    def update_heartbeat_timestamp(self):
        pass


def build_cluster(node_ids):
    """Cria nós Bully conectados por um barramento em memória."""
    bus = InMemoryBus()
    peers = {nid: (f"node{nid}", 5000) for nid in node_ids}
    nodes = {}

    for nid in node_ids:
        other_peers = {pid: addr for pid, addr in peers.items() if pid != nid}
        bully = BullyElection(
            node_id=nid,
            peers=other_peers,
            network=bus,
            election_timeout=0.2,
        )
        nodes[nid] = {"bully": bully, "hb": HeartbeatStub()}

    async def make_router(nid):
        bully = nodes[nid]["bully"]
        hb = nodes[nid]["hb"]

        async def router(msg):
            t = msg["type"]
            sid = msg["sender_id"]
            if t == "ELECTION":
                await bully.handle_election_msg(sid)
            elif t == "ANSWER":
                await bully.handle_answer_msg()
            elif t == "COORDINATOR":
                bully.abdicate_if_higher_leader(sid)
                await bully.handle_coordinator_msg(sid, hb)

        return router

    async def setup():
        for nid in node_ids:
            bus.register(nid, await make_router(nid))

    return bus, nodes, setup


class TestIntegratedSystem(unittest.IsolatedAsyncioTestCase):
    async def test_full_failover_cycle(self):
        """Testa o ciclo completo: falha do líder detectada pelo Heartbeat aciona eleição Bully."""
        mock_net = AsyncMock()
        peers = {1: ("node1", 5000), 3: ("node3", 5000)}
        bully = BullyElection(node_id=2, peers=peers, network=mock_net, election_timeout=0.1)
        hb = HeartbeatDetector(node_id=2, network=mock_net, interval=0.05, timeout=0.1)
        bully.leader_id = 3

        # Dispara manualmente a expiração simulando timeout do heartbeat
        await bully.start_election()

        # Como o nó 3 não respondeu (mocked net não envia ANSWER), nó 2 deve vencer
        self.assertTrue(bully.is_leader())
        self.assertEqual(bully.leader_id, 2)

    async def test_safety_unique_leader_highest_id(self):
        """Safety: após a eleição deve existir exatamente um líder no cluster, o de maior ID."""
        bus, nodes, setup = build_cluster([1, 2, 3])
        await setup()

        await nodes[3]["bully"].bootstrap(delay=0.0)
        await asyncio.sleep(0.3)

        leaders = [nid for nid, st in nodes.items() if st["bully"].is_leader()]
        self.assertEqual(leaders, [3])
        for st in nodes.values():
            self.assertEqual(st["bully"].leader_id, 3)

    async def test_liveness_new_leader_when_highest_crashes(self):
        """Liveness: com o nó 3 falho, o nó 2 deve assumir a liderança eventualmente."""
        bus, nodes, setup = build_cluster([1, 2, 3])
        await setup()
        await nodes[3]["bully"].bootstrap(delay=0.0)
        await asyncio.sleep(0.2)

        bus.crash(3)
        await nodes[1]["bully"].start_election()
        await nodes[2]["bully"].start_election()
        await asyncio.sleep(0.4)

        # Apenas nós online contam como líderes ativos
        leaders = [
            nid for nid, st in nodes.items()
            if nid not in bus.offline and st["bully"].is_leader()
        ]
        self.assertEqual(leaders, [2])
        self.assertEqual(nodes[1]["bully"].leader_id, 2)

    async def test_crash_recovery_highest_id_reclaims_leadership(self):
        """Crash-Recovery: ao reiniciar, o nó de maior ID reassume a coordenação sem split-brain."""
        bus, nodes, setup = build_cluster([1, 2, 3])
        await setup()
        await nodes[3]["bully"].bootstrap(delay=0.0)
        await asyncio.sleep(0.2)

        # Nó 3 falha e nó 2 assume
        bus.crash(3)
        await nodes[2]["bully"].start_election()
        await asyncio.sleep(0.3)
        self.assertTrue(nodes[2]["bully"].is_leader())

        # Nó 3 reinicia e convoca eleição de recuperação
        bus.recover(3)
        nodes[3]["bully"].is_electing = False
        await nodes[3]["bully"].bootstrap(delay=0.0)
        await asyncio.sleep(0.3)

        leaders = [nid for nid, st in nodes.items() if st["bully"].is_leader()]
        self.assertEqual(leaders, [3])
        self.assertEqual(nodes[2]["bully"].leader_id, 3)

    async def test_message_complexity_is_bounded(self):
        """Complexidade de Mensagens: uma eleição completa troca mensagens O(N^2) no pior caso."""
        bus, nodes, setup = build_cluster([1, 2, 3])
        await setup()
        await nodes[3]["bully"].bootstrap(delay=0.0)
        await asyncio.sleep(0.3)

        # Limite superior conservador para 3 nós
        self.assertLessEqual(bus.messages, 3 * (3 - 1) + 3)


if __name__ == "__main__":
    unittest.main()
