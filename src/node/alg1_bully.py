import asyncio
import logging
import time


def _epoch() -> float:
    """Timestamp de parede (epoch) usado na instrumentação."""
    return time.time()


class BullyElection:
    """Algoritmo do Valentão (Bully) para eleição de líder.

    Hipóteses: canais TCP confiáveis e FIFO; modelo parcialmente síncrono com
    limiar de timeout para respostas. O nó de maior ID entre os nós vivos é
    eleito coordenador. Ao reiniciar, um nó dispara uma nova eleição, de modo
    que o maior ID vivo (crash-recovery) reassume a liderança.
    """

    def __init__(
        self,
        node_id: int,
        peers: dict,
        network: object,
        election_timeout: float = 2.0,
    ):
        self.node_id = node_id
        self.peers = peers
        self.net = network
        self.election_timeout = election_timeout
        self.leader_id = max(list(peers.keys()) + [node_id])
        self.is_electing = False
        self.received_answer = False
        self.election_msgs_sent = 0

    def is_leader(self) -> bool:
        return self.leader_id == self.node_id

    async def bootstrap(self, delay: float = 2.0):
        """Eleição de inicialização/recuperação (crash-recovery).

        Executada após um pequeno atraso para que todos os servidores TCP já
        estejam aceitando conexões. Se o nó acredita ser o de maior ID, ele se
        proclama coordenador diretamente (permitindo que um nó reiniciado com o
        maior ID reassuma a liderança). Caso contrário, aguarda os heartbeats do
        líder; se estes não chegarem, o detector de falhas inicia a eleição.
        """
        await asyncio.sleep(delay)
        if self.is_leader():
            await self._announce_coordination()

    async def _send(self, pid: int, kind: str):
        """Envia uma mensagem de eleição e registra a métrica de mensagens."""
        host, port = self.peers[pid]
        self.election_msgs_sent += 1
        logging.info(
            f"ELECTION_MSG type={kind} from={self.node_id} to={pid} "
            f"epoch={_epoch():.6f}"
        )
        asyncio.create_task(
            self.net.send_message(host, port, {"type": kind, "sender_id": self.node_id})
        )

    async def start_election(self):
        if self.is_electing:
            return

        self.is_electing = True
        self.received_answer = False
        self.election_msgs_sent = 0
        logging.info(f"ELECTION_START node={self.node_id} epoch={_epoch():.6f}")
        higher_peers = [pid for pid in self.peers if pid > self.node_id]

        if not higher_peers:
            await self._announce_coordination()
            return

        for pid in higher_peers:
            await self._send(pid, "ELECTION")

        await asyncio.sleep(self.election_timeout)

        # Só anuncia se ainda estiver em eleição e ninguém com ID maior respondeu.
        # Se um COORDINATOR foi recebido durante a espera, is_electing já é False.
        if self.is_electing and not self.received_answer:
            await self._announce_coordination()

    async def handle_election_msg(self, sender_id: int):
        if self.node_id > sender_id:
            if sender_id in self.peers:
                await self._send(sender_id, "ANSWER")
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
            f"LEADER_ADOPTED node={self.node_id} leader={self.leader_id} "
            f"election_msgs={self.election_msgs_sent} epoch={_epoch():.6f}"
        )

    async def _announce_coordination(self):
        self.leader_id = self.node_id
        self.is_electing = False
        logging.info(
            f"LEADER_ANNOUNCE node={self.node_id} epoch={_epoch():.6f} "
            f"election_msgs={self.election_msgs_sent}"
        )
        for pid in self.peers:
            await self._send(pid, "COORDINATOR")
