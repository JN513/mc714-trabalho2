"""Testes do terceiro algoritmo (bônus): Two-Phase Commit (2PC).

Cobrem as transições unitárias (travas, votos, decisões) e, por meio de uma
rede em memória, as propriedades de atomicidade sob conflito, sob voto negativo
e sob falha do coordenador (protocolo de terminação cooperativo).
"""

import sys
import os
import time
import asyncio

import pytest

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/node")))

from alg3_2pc import TwoPhaseCommit, KeyValueStore


# --------------------------------------------------------------------------- #
# Testes unitários
# --------------------------------------------------------------------------- #
def test_kv_lock_commit_abort():
    kv = KeyValueStore()
    assert kv.try_lock("t1", [("x", 1)]) is True
    # Outra transação não consegue travar a mesma chave.
    assert kv.try_lock("t2", [("x", 2)]) is False
    kv.commit("t1", [("x", 1)])
    assert kv.snapshot() == {"x": 1}
    assert kv.locks == {}

    assert kv.try_lock("t3", [("y", 5)]) is True
    kv.abort("t3", [("y", 5)])
    assert kv.snapshot() == {"x": 1}
    assert kv.locks == {}


@pytest.mark.asyncio
async def test_participant_votes_yes_then_no_on_conflict():
    net = _NullNet()
    tpc = TwoPhaseCommit(1, {2: ("node2", 5000)}, net, KeyValueStore(), lambda: 2)
    await tpc.handle_prepare("t1", [("x", 1)], coord_id=2)
    assert net.sent[-1]["vote"] == "YES"
    # Segunda transação sobre a mesma chave -> NO (conflito de trava).
    await tpc.handle_prepare("t2", [("x", 2)], coord_id=2)
    assert net.sent[-1]["vote"] == "NO"


class _NullNet:
    def __init__(self):
        self.sent = []

    async def send_message(self, host, port, payload):
        self.sent.append(payload)
        return True


# --------------------------------------------------------------------------- #
# Testes de integração com rede em memória
# --------------------------------------------------------------------------- #
class Bus:
    def __init__(self):
        self.handlers = {}
        self.offline = set()

    def register(self, nid, handler):
        self.handlers[nid] = handler

    def crash(self, nid):
        self.offline.add(nid)

    async def send_message(self, host, port, payload):
        target = int("".join(filter(str.isdigit, host)))
        if target in self.offline or target not in self.handlers:
            return False
        await self.handlers[target](payload)
        return True


def build_cluster(node_ids=(1, 2, 3), leader=3):
    bus = Bus()
    peers = {nid: (f"node{nid}", 5000) for nid in node_ids}
    nodes = {}
    for nid in node_ids:
        other = {pid: addr for pid, addr in peers.items() if pid != nid}
        nodes[nid] = TwoPhaseCommit(
            nid, other, bus, KeyValueStore(),
            get_leader_id=lambda leader=leader: leader,
            vote_timeout=0.3, decision_timeout=0.3,
        )

    async def make_router(nid):
        tpc = nodes[nid]

        async def router(msg):
            t = msg["type"]
            sid = msg.get("sender_id")
            if t == "PREPARE":
                await tpc.handle_prepare(msg["txn_id"], msg["ops"], sid)
            elif t == "VOTE":
                await tpc.handle_vote(msg["txn_id"], msg["vote"], sid)
            elif t == "DECISION":
                await tpc.handle_decision(msg["txn_id"], msg["decision"])
            elif t == "QUERY":
                await tpc.handle_query(msg["txn_id"], sid)
            elif t == "STATE":
                await tpc.handle_state(msg["txn_id"], msg["state"], sid)
            elif t == "TXN":
                await tpc.handle_client_txn(msg["ops"], sid)

        return router

    async def setup():
        for nid in node_ids:
            bus.register(nid, await make_router(nid))

    return bus, nodes, setup


@pytest.mark.asyncio
async def test_2pc_all_commit_and_atomic():
    bus, nodes, setup = build_cluster()
    await setup()
    decision = await nodes[3].coordinate([("x", 1), ("y", 2)])
    assert decision == "COMMIT"
    for nid in (1, 2, 3):
        assert nodes[nid].kv.snapshot() == {"x": 1, "y": 2}


@pytest.mark.asyncio
async def test_2pc_abort_on_conflict_is_atomic():
    bus, nodes, setup = build_cluster()
    await setup()
    # O nó 1 mantém a chave "x" travada por outra transação.
    nodes[1].kv.try_lock("outra", [("x", 99)])
    decision = await nodes[3].coordinate([("x", 1)])
    assert decision == "ABORT"
    for nid in (1, 2, 3):
        assert "x" not in nodes[nid].kv.snapshot()


@pytest.mark.asyncio
async def test_2pc_abort_on_negative_vote():
    bus, nodes, setup = build_cluster()
    await setup()
    nodes[2].participant_vote_no = True
    decision = await nodes[3].coordinate([("z", 7)])
    assert decision == "ABORT"
    for nid in (1, 2, 3):
        assert "z" not in nodes[nid].kv.snapshot()


@pytest.mark.asyncio
async def test_2pc_termination_aborts_when_no_one_committed():
    bus, nodes, setup = build_cluster()
    await setup()
    txn = "9.1"
    old = time.time() - 10
    for nid in (1, 2, 3):
        nodes[nid].prepared[txn] = ([("a", 1)], 3, old)
    # Nó 1 (participante) inicia o protocolo de terminação.
    await nodes[1]._terminate(txn)
    assert nodes[1].decisions[txn] == "ABORT"
    for nid in (1, 2, 3):
        assert "a" not in nodes[nid].kv.snapshot()


@pytest.mark.asyncio
async def test_2pc_termination_commits_if_someone_committed():
    bus, nodes, setup = build_cluster()
    await setup()
    txn = "9.2"
    old = time.time() - 10
    for nid in (1, 2, 3):
        nodes[nid].prepared[txn] = ([("a", 1)], 3, old)
    # O nó 2 já recebeu COMMIT antes da falha do coordenador.
    nodes[2].decisions[txn] = "COMMIT"
    nodes[2].kv.commit(txn, [("a", 1)])
    nodes[2].prepared.pop(txn)

    await nodes[1]._terminate(txn)
    assert nodes[1].decisions[txn] == "COMMIT"
    for nid in (1, 2, 3):
        assert nodes[nid].kv.snapshot().get("a") == 1


@pytest.mark.asyncio
async def test_2pc_termination_blocks_if_peer_unreachable():
    bus, nodes, setup = build_cluster()
    await setup()
    txn = "9.3"
    old = time.time() - 10
    for nid in (1, 2, 3):
        nodes[nid].prepared[txn] = ([("a", 1)], 3, old)
    bus.crash(3)  # um par inacessível
    await nodes[1]._terminate(txn)
    # Permanece bloqueado: nenhuma decisão tomada, trava mantida.
    assert txn not in nodes[1].decisions
    assert txn in nodes[1].prepared
