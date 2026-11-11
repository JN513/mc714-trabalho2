"""Terceiro algoritmo (bônus): Two-Phase Commit (2PC) integrado ao líder.

O coordenador do 2PC é o próprio líder eleito pelo Algoritmo do Valentão
(``alg1_bully``). Clientes enviam transações (conjunto de operações
chave-valor) ao coordenador, que executa as duas fases:

  1. **PREPARE/votação:** o coordenador envia ``PREPARE`` a todos os
     participantes; cada um tenta travar as chaves e responde ``VOTE``
     (``YES``/``NO``).
  2. **DECISÃO:** se todos votaram ``YES`` (e o próprio coordenador conseguiu
     travar), decide ``COMMIT``; caso contrário, ``ABORT``. A decisão é
     propagada a todos, que aplicam ou descartam as operações e liberam as
     travas.

Propriedades: **atomicidade** (todos aplicam ou todos descartam) e
**uniformidade** (não há decisões divergentes). Limitação clássica: se o
coordenador falha depois do ``PREPARE`` e antes da decisão, os participantes
ficam *bloqueados* segurando travas. Implementa-se um **protocolo de terminação
cooperativo**: após um tempo, um participante preparado consulta os demais; se
alguém já efetivou (``COMMIT``), adota essa decisão; se todos respondem e
ninguém efetivou, aborta com segurança. Se algum nó está inacessível, o
participante permanece bloqueado --- o 2PC não é não-bloqueante.
"""

import os
import json
import time
import asyncio
import logging


def _epoch() -> float:
    return time.time()


class KeyValueStore:
    """Armazenamento chave-valor em memória com travas por transação."""

    def __init__(self):
        self.data = {}
        self.locks = {}

    def try_lock(self, txn_id, ops) -> bool:
        keys = [k for k, _ in ops]
        for k in keys:
            owner = self.locks.get(k)
            if owner is not None and owner != txn_id:
                return False
        for k in keys:
            self.locks[k] = txn_id
        return True

    def commit(self, txn_id, ops):
        for k, v in ops:
            self.data[k] = v
        self._release(txn_id, ops)

    def abort(self, txn_id, ops):
        self._release(txn_id, ops)

    def _release(self, txn_id, ops):
        for k, _ in ops:
            if self.locks.get(k) == txn_id:
                del self.locks[k]

    def snapshot(self) -> dict:
        return dict(self.data)


class TwoPhaseCommit:
    def __init__(
        self,
        node_id: int,
        peers: dict,
        network: object,
        kv: KeyValueStore,
        get_leader_id,
        vote_timeout: float = 2.0,
        decision_timeout: float = 3.0,
        crash_after_prepare: bool = False,
        participant_vote_no: bool = False,
    ):
        self.node_id = node_id
        self.peers = peers
        self.net = network
        self.kv = kv
        self.get_leader_id = get_leader_id
        self.vote_timeout = vote_timeout
        self.decision_timeout = decision_timeout
        self.crash_after_prepare = crash_after_prepare
        self.participant_vote_no = participant_vote_no

        self.txn_counter = 0
        self.votes = {}        # txn_id -> {voter_id: voto}
        self.vote_events = {}  # txn_id -> asyncio.Event (todos os votos recebidos)
        self.decisions = {}    # txn_id -> "COMMIT"/"ABORT"
        self.prepared = {}     # txn_id -> (ops, coord_id, timestamp)
        self.states = {}       # txn_id -> {reporter_id: estado} (terminação)
        self.results = []      # histórico de (txn_id, decisão, duração)
        self.commits = 0
        self.aborts = 0
        self._terminating = set()

    # ------------------------------------------------------------------ #
    def next_txn_id(self) -> str:
        self.txn_counter += 1
        return f"{self.node_id}.{self.txn_counter}"

    async def _send(self, pid: int, kind: str, **fields) -> bool:
        if pid not in self.peers:
            return False
        host, port = self.peers[pid]
        payload = {"type": kind, "sender_id": self.node_id, **fields}
        return await self.net.send_message(host, port, payload)

    # ------------------------------------------------------------------ #
    # Coordenador
    # ------------------------------------------------------------------ #
    async def coordinate(self, ops) -> str:
        txn_id = self.next_txn_id()
        t0 = time.time()
        self.votes[txn_id] = {}
        self.vote_events[txn_id] = asyncio.Event()
        participants = list(self.peers.keys())
        logging.info(
            f"TXN_START txn={txn_id} coord={self.node_id} "
            f"epoch={_epoch():.6f} ops={len(ops)}"
        )

        local_ok = await self._prepare_local(txn_id, ops)
        await asyncio.gather(
            *[self._send(pid, "PREPARE", txn_id=txn_id, ops=ops) for pid in participants]
        )

        if self.crash_after_prepare:
            logging.warning(f"TXN_COORD_CRASH txn={txn_id} epoch={_epoch():.6f}")
            os._exit(1)

        if local_ok:
            try:
                await asyncio.wait_for(
                    self.vote_events[txn_id].wait(), timeout=self.vote_timeout
                )
            except asyncio.TimeoutError:
                pass
            votes = self.votes[txn_id]
            all_yes = len(votes) == len(participants) and all(
                v == "YES" for v in votes.values()
            )
        else:
            all_yes = False

        decision = "COMMIT" if all_yes else "ABORT"
        await self._decide(txn_id, decision, participants)
        duration = time.time() - t0
        self.results.append((txn_id, decision, duration))
        logging.info(
            f"TXN_RESULT txn={txn_id} decision={decision} duration={duration:.4f} "
            f"epoch={_epoch():.6f} commits={self.commits} aborts={self.aborts}"
        )
        return decision

    async def _prepare_local(self, txn_id, ops) -> bool:
        if self.kv.try_lock(txn_id, ops):
            self.prepared[txn_id] = (ops, self.node_id, time.time())
            return True
        return False

    async def _decide(self, txn_id, decision, participants):
        await self._apply_decision(txn_id, decision)
        await asyncio.gather(
            *[self._send(pid, "DECISION", txn_id=txn_id, decision=decision)
              for pid in participants]
        )

    async def _apply_decision(self, txn_id, decision):
        self.decisions[txn_id] = decision
        if decision == "COMMIT":
            self.commits += 1
        else:
            self.aborts += 1
        if txn_id in self.prepared:
            ops, _, _ = self.prepared.pop(txn_id)
            if decision == "COMMIT":
                self.kv.commit(txn_id, ops)
            else:
                self.kv.abort(txn_id, ops)

    # ------------------------------------------------------------------ #
    # Participante
    # ------------------------------------------------------------------ #
    async def handle_prepare(self, txn_id, ops, coord_id):
        if self.participant_vote_no:
            vote = "NO"
        elif self.kv.try_lock(txn_id, ops):
            self.prepared[txn_id] = (ops, coord_id, time.time())
            vote = "YES"
        else:
            vote = "NO"
        logging.info(
            f"TXN_VOTE txn={txn_id} node={self.node_id} vote={vote} "
            f"epoch={_epoch():.6f}"
        )
        await self._send(coord_id, "VOTE", txn_id=txn_id, vote=vote)

    async def handle_vote(self, txn_id, vote, voter_id):
        self.votes.setdefault(txn_id, {})[voter_id] = vote
        ev = self.vote_events.get(txn_id)
        if ev is not None and len(self.votes[txn_id]) >= len(self.peers):
            ev.set()

    async def handle_decision(self, txn_id, decision):
        await self._apply_decision(txn_id, decision)
        logging.info(
            f"TXN_DECISION txn={txn_id} node={self.node_id} "
            f"decision={decision} epoch={_epoch():.6f}"
        )

    # ------------------------------------------------------------------ #
    # Protocolo de terminação cooperativo (falha do coordenador)
    # ------------------------------------------------------------------ #
    async def run_termination_monitor(self):
        while True:
            await asyncio.sleep(max(self.decision_timeout / 2.0, 0.2))
            now = time.time()
            for txn_id, (_, _, ts) in list(self.prepared.items()):
                if txn_id in self.decisions or txn_id in self._terminating:
                    continue
                if now - ts > self.decision_timeout:
                    asyncio.create_task(self._terminate(txn_id))

    async def _terminate(self, txn_id):
        self._terminating.add(txn_id)
        peers = [p for p in self.peers if p != self.node_id]
        self.states[txn_id] = {}
        await asyncio.gather(*[self._send(p, "QUERY", txn_id=txn_id) for p in peers])

        deadline = time.time() + self.decision_timeout
        while time.time() < deadline and len(self.states[txn_id]) < len(peers):
            await asyncio.sleep(0.05)

        states = self.states[txn_id]
        if any(s == "COMMITTED" for s in states.values()):
            decision = "COMMIT"
        elif len(states) == len(peers):
            # Todos responderam e nenhum efetivou: abortar é seguro.
            decision = "ABORT"
        else:
            # Algum nó inacessível -> permanece bloqueado (limitação do 2PC).
            self._terminating.discard(txn_id)
            return

        logging.info(
            f"TXN_TERMINATE txn={txn_id} node={self.node_id} "
            f"decision={decision} epoch={_epoch():.6f}"
        )
        await self._apply_decision(txn_id, decision)
        await asyncio.gather(
            *[self._send(p, "DECISION", txn_id=txn_id, decision=decision) for p in peers]
        )

    async def handle_query(self, txn_id, asker_id):
        if self.decisions.get(txn_id) == "COMMIT":
            state = "COMMITTED"
        elif self.decisions.get(txn_id) == "ABORT":
            state = "ABORTED"
        elif txn_id in self.prepared:
            state = "PREPARED"
        else:
            state = "UNKNOWN"
        await self._send(asker_id, "STATE", txn_id=txn_id, state=state)

    async def handle_state(self, txn_id, state, reporter_id):
        self.states.setdefault(txn_id, {})[reporter_id] = state

    # ------------------------------------------------------------------ #
    # Entrada de cliente e observabilidade
    # ------------------------------------------------------------------ #
    async def handle_client_txn(self, ops, sender_id):
        if self.get_leader_id() != self.node_id:
            await self._send(self.get_leader_id(), "TXN", ops=ops)
            return
        await self.coordinate(ops)

    async def handle_snapshot(self):
        logging.info(
            f"SNAPSHOT node={self.node_id} "
            f"data={json.dumps(self.kv.snapshot(), sort_keys=True)} "
            f"epoch={_epoch():.6f}"
        )
