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
    eleito coordenador. Ao reiniciar, um nó convoca ou reassume a liderança, de modo
    que o maior ID vivo (crash-recovery) volta a coordenar o cluster sem split-brain.
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
        all_ids = list(peers.keys()) + [node_id]
        self.leader_id = max(all_ids) if all_ids else node_id
        self.is_electing = False
        self.received_answer = False
        self.election_start_time = 0.0
        self.last_election_duration = 0.0
        self.election_msgs_sent = 0

    def is_leader(self) -> bool:
        return self.leader_id == self.node_id

    async def bootstrap(self, delay: float = 2.0):
        """Eleição de inicialização/recuperação (crash-recovery).

        Executada após um pequeno atraso para que os servidores TCP vizinhos
        estejam aceitando conexões. Se o nó for o de maior ID, proclama liderança
        diretamente. Caso contrário, aguarda heartbeats do coordenador.
        """
        await asyncio.sleep(delay)
        if self.is_leader():
            await self._announce_coordination()
        elif not self.leader_id:
            await self.start_election()

    async def _send(self, pid: int, kind: str):
        """Envia uma mensagem de eleição e atualiza as métricas de tráfego."""
        if pid not in self.peers:
            return
        host, port = self.peers[pid]
        self.election_msgs_sent += 1
        logging.info(
            f"ELECTION_MSG type={kind} from={self.node_id} to={pid} "
            f"epoch={_epoch():.6f}"
        )
        asyncio.create_task(
            self.net.send_message(
                host, port, {"type": kind, "sender_id": self.node_id}
            )
        )

    async def start_election(self):
        """Inicia o processo de eleição convocando nós com ID superior."""
        if self.is_electing:
            return

        self.is_electing = True
        self.received_answer = False
        self.election_msgs_sent = 0
        self.election_start_time = time.time()
        logging.info(
            f"ELECTION_START node={self.node_id} epoch={_epoch():.6f} | "
            f"Node {self.node_id}: Iniciando processo de eleição..."
        )

        higher_peers = [pid for pid in self.peers if pid > self.node_id]

        # Se não há nós com ID superior no cluster, este nó vence imediatamente
        if not higher_peers:
            await self._announce_coordination()
            return

        for pid in higher_peers:
            await self._send(pid, "ELECTION")

        # Aguarda prazo para recebimento de ANSWER
        await asyncio.sleep(self.election_timeout)

        # Se nenhum nó superior respondeu e ainda estamos elegendo, assume liderança
        if self.is_electing and not self.received_answer:
            await self._announce_coordination()

    async def handle_election_msg(self, sender_id: int):
        """Processa mensagem ELECTION recebida de outro nó."""
        if self.node_id > sender_id:
            # Responde ANSWER para o nó inferior confirmando vivacidade
            if sender_id in self.peers:
                await self._send(sender_id, "ANSWER")

            # Se este nó for o líder atual, reanuncia coordenação
            if self.is_leader():
                asyncio.create_task(self._announce_coordination())
            elif not self.is_electing:
                # Inicia sua própria eleição para disputar liderança com superiores
                asyncio.create_task(self.start_election())

    async def handle_answer_msg(self):
        """Processa mensagem ANSWER indicando que nó de maior ID assumiu a disputa."""
        self.received_answer = True
        logging.info(f"Node {self.node_id}: Recebeu ANSWER de nó superior.")

    async def handle_coordinator_msg(
        self, new_leader_id: int, heartbeat_detector: object
    ):
        """Processa mensagem de anúncio de vitória de um coordenador."""
        # Se um nó inferior tentar se proclamar líder, rejeita e desafia
        if new_leader_id < self.node_id:
            logging.warning(
                f"Node {self.node_id}: Rejeitou anúncio de líder inferior Node {new_leader_id}. Disparando eleição."
            )
            asyncio.create_task(self.start_election())
            return

        self.leader_id = new_leader_id
        self.is_electing = False
        self.received_answer = False
        if self.election_start_time > 0:
            self.last_election_duration = time.time() - self.election_start_time
            self.election_start_time = 0.0

        heartbeat_detector.update_heartbeat_timestamp()
        logging.info(
            f"LEADER_ADOPTED node={self.node_id} leader={self.leader_id} "
            f"election_msgs={self.election_msgs_sent} epoch={_epoch():.6f} | "
            f"Node {self.node_id}: Novo líder reconhecido -> Node {self.leader_id}"
        )

    def abdicate_if_higher_leader(self, candidate_id: int):
        """Abdica da liderança caso um nó com ID superior seja identificado (evita split-brain)."""
        if candidate_id > self.node_id and self.is_leader():
            logging.info(
                f"Node {self.node_id}: Abdicando da liderança em favor do Node {candidate_id} (ID superior)."
            )
            self.leader_id = candidate_id
            self.is_electing = False

    async def _announce_coordination(self):
        """Consagra a vitória deste nó e anuncia para todos os membros do cluster."""
        self.leader_id = self.node_id
        self.is_electing = False
        if self.election_start_time > 0:
            self.last_election_duration = time.time() - self.election_start_time
            self.election_start_time = 0.0

        logging.info(
            f"LEADER_ANNOUNCE node={self.node_id} epoch={_epoch():.6f} "
            f"election_msgs={self.election_msgs_sent} | "
            f"Node {self.node_id}: >>> VENCEU A ELEIÇÃO E ASSUMIU COMO LÍDER <<<"
        )
        for pid in self.peers:
            await self._send(pid, "COORDINATOR")
