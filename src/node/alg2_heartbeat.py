import asyncio
import time
import logging


class HeartbeatDetector:
    """Detector de falhas baseado em heartbeat.

    O líder envia mensagens ``HEARTBEAT`` a cada ``interval`` segundos. Cada
    seguidor registra o instante do último heartbeat recebido do líder atual e,
    caso o intervalo exceda ``timeout``, considera o líder falho (modelo
    crash-stop) e dispara o callback de eleição.
    """

    def __init__(
        self, node_id: int, network: object, interval: float = 1.0, timeout: float = 3.0
    ):
        self.node_id = node_id
        self.net = network
        self.interval = interval
        self.timeout = timeout
        self.last_heartbeat = time.time()

    async def run_sender(self, is_leader_func, peers: dict):
        """Dispara heartbeats periodicamente para os seguidores se este nó for o líder."""
        while True:
            await asyncio.sleep(self.interval)
            if is_leader_func():
                for pid, (host, port) in peers.items():
                    asyncio.create_task(
                        self.net.send_message(
                            host, port, {"type": "HEARTBEAT", "sender_id": self.node_id}
                        )
                    )

    async def run_checker(self, is_leader_func, is_electing_func, on_timeout_callback):
        """Verifica se o tempo desde o último heartbeat do líder excedeu o limite."""
        while True:
            await asyncio.sleep(0.5)
            if not is_leader_func() and not is_electing_func():
                elapsed = time.time() - self.last_heartbeat
                if elapsed > self.timeout:
                    logging.warning(
                        f"HEARTBEAT_TIMEOUT node={self.node_id} "
                        f"epoch={time.time():.6f} elapsed={elapsed:.3f} "
                        f"timeout={self.timeout:.3f}"
                    )
                    await on_timeout_callback()

    def update_heartbeat_timestamp(self):
        self.last_heartbeat = time.time()
