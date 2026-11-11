"""Testes de integração dos algoritmos usando uma rede em memória.

A rede em memória substitui o TCP por entrega assíncrona direta entre os
handlers dos nós, permitindo verificar as propriedades de correção (segurança,
vivacidade e recuperação) sem depender de contêineres.
"""

import sys
import os
import asyncio

import pytest

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
            election_timeout=0.1,
        )
        nodes[nid] = {"bully": bully, "hb": HeartbeatStub()}

    async def make_router(nid):
        bully = nodes[nid]["bully"]
        hb = nodes[nid]["hb"]

        async def router(msg):
            t = msg.get("type")
            if t == "ELECTION":
                await bully.handle_election_msg(msg["sender_id"])
            elif t == "ANSWER":
                await bully.handle_answer_msg()
            elif t == "COORDINATOR":
                await bully.handle_coordinator_msg(msg["sender_id"], hb)

        return router

    async def setup():
        for nid in node_ids:
            bus.register(nid, await make_router(nid))

    return bus, nodes, setup


@pytest.mark.asyncio
async def test_safety_single_leader_after_election():
    """Após a eleição deve existir exatamente um líder, o de maior ID."""
    bus, nodes, setup = build_cluster([1, 2, 3])
    await setup()

    await nodes[3]["bully"].bootstrap(delay=0.0)
    await asyncio.sleep(0.5)

    leaders = [nid for nid, st in nodes.items() if st["bully"].is_leader()]
    assert leaders == [3]
    for st in nodes.values():
        assert st["bully"].leader_id == 3


@pytest.mark.asyncio
async def test_liveness_new_leader_when_highest_crashes():
    """Com o nó 3 falho, o nó 2 deve assumir a liderança."""
    bus, nodes, setup = build_cluster([1, 2, 3])
    await setup()
    await nodes[3]["bully"].bootstrap(delay=0.0)
    await asyncio.sleep(0.3)

    bus.crash(3)
    await nodes[1]["bully"].start_election()
    await nodes[2]["bully"].start_election()
    await asyncio.sleep(0.5)

    # Apenas nós online contam como líderes.
    leaders = [
        nid for nid, st in nodes.items() if nid not in bus.offline and st["bully"].is_leader()
    ]
    assert leaders == [2]
    assert nodes[1]["bully"].leader_id == 2


@pytest.mark.asyncio
async def test_crash_recovery_highest_id_reclaims_leadership():
    """Ao reiniciar, o nó de maior ID reassume a coordenação."""
    bus, nodes, setup = build_cluster([1, 2, 3])
    await setup()
    await nodes[3]["bully"].bootstrap(delay=0.0)
    await asyncio.sleep(0.3)

    # Nó 3 falha e nó 2 assume.
    bus.crash(3)
    await nodes[2]["bully"].start_election()
    await asyncio.sleep(0.4)
    assert nodes[2]["bully"].is_leader()

    # Nó 3 reinicia e dispara eleição de recuperação.
    bus.recover(3)
    nodes[3]["bully"].is_electing = False
    await nodes[3]["bully"].bootstrap(delay=0.0)
    await asyncio.sleep(0.5)

    leaders = [nid for nid, st in nodes.items() if st["bully"].is_leader()]
    assert leaders == [3]
    assert nodes[2]["bully"].leader_id == 3


@pytest.mark.asyncio
async def test_message_complexity_is_bounded():
    """Uma eleição completa troca um número de mensagens O(N^2) no pior caso."""
    bus, nodes, setup = build_cluster([1, 2, 3])
    await setup()
    await nodes[3]["bully"].bootstrap(delay=0.0)
    await asyncio.sleep(0.4)

    # Para 3 nós, o custo máximo teórico é (N-1) mensagens ELECTION + respostas
    # + (N-1) COORDINATOR. Verifica-se um limite superior conservador.
    assert bus.messages <= 3 * (3 - 1) + 3
