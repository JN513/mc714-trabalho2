import asyncio
import time
import logging
import os


class HeartbeatDetector:
    """Detector de falhas baseado em heartbeat.

    O líder envia mensagens HEARTBEAT a cada ``interval`` segundos. Cada
    seguidor registra o instante do último heartbeat recebido do líder atual e,
    caso o intervalo decorrido exceda ``timeout``, aciona reativamente o
    callback para iniciar uma nova eleição.
    """

    def __init__(
        self,
        node_id: int,
        network: object,
        interval: float = None,
        timeout: float = None,
    ):
        self.node_id = node_id
        self.net = network
        self.interval = (
            interval
            if interval is not None
            else float(os.getenv("HEARTBEAT_INTERVAL", "1.0"))
        )
        self.timeout = (
            timeout
            if timeout is not None
            else float(os.getenv("HEARTBEAT_TIMEOUT", "3.0"))
        )
        self.last_heartbeat = time.time()
        self.last_detection_latency = 0.0

    async def run_sender(self, is_leader_func, peers: dict):
        """Dispara heartbeats periodicamente para todos os nós se for o líder."""
        while True:
            await asyncio.sleep(self.interval)
            if is_leader_func():
                for pid, (host, port) in peers.items():
                    asyncio.create_task(
                        self.net.send_message(
                            host,
                            port,
                            {"type": "HEARTBEAT", "sender_id": self.node_id},
                        )
                    )

    async def run_checker(
        self, is_leader_func, is_electing_func, on_timeout_callback
    ):
        """Monitora ausência de heartbeat do líder e dispara eleição em caso de timeout."""
        while True:
            await asyncio.sleep(min(0.2, self.interval / 2))
            if not is_leader_func() and not is_electing_func():
                elapsed = time.time() - self.last_heartbeat
                if elapsed > self.timeout:
                    self.last_detection_latency = elapsed
                    now_epoch = time.time()
                    logging.warning(
                        f"HEARTBEAT_TIMEOUT node={self.node_id} "
                        f"epoch={now_epoch:.6f} elapsed={elapsed:.3f} "
                        f"timeout={self.timeout:.3f} | "
                        f"Node {self.node_id}: Timeout de heartbeat ({elapsed:.2f}s)! Iniciando eleição."
                    )
                    # Reseta timestamp local para prevenir tempestades de disparos reentrantes
                    self.last_heartbeat = time.time()
                    await on_timeout_callback()

    def update_heartbeat_timestamp(self):
        """Atualiza timestamp do último sinal de vivacidade do coordenador."""
        self.last_heartbeat = time.time()
