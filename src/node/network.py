import asyncio
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


class NetworkManager:
    """Camada de comunicação assíncrona ponto a ponto sobre TCP.

    Cada mensagem é um documento JSON delimitado por uma conexão TCP de vida
    curta (abre, escreve, fecha). A camada implementa timeouts e deadlines
    explícitos conforme exigido pelo enunciado do trabalho e mantém contadores
    de tráfego para medição da complexidade de mensagens.
    """

    def __init__(self, node_id: int, port: int, handler_callback):
        self.node_id = node_id
        self.port = port
        self.handler_callback = handler_callback
        self.server = None
        # Contadores de tráfego (instrumentação para análise experimental)
        self.messages_sent = 0
        self.messages_received = 0
        self.msg_counts = {
            "ELECTION": 0,
            "ANSWER": 0,
            "COORDINATOR": 0,
            "HEARTBEAT": 0,
            "TXN": 0,
            "PREPARE": 0,
            "VOTE": 0,
            "DECISION": 0,
            "QUERY": 0,
            "STATE": 0,
            "SNAPSHOT": 0,
        }

    async def start_server(self):
        self.server = await asyncio.start_server(
            self._handle_client, "0.0.0.0", self.port
        )
        logging.info(f"Node {self.node_id} escutando na porta {self.port}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            data = await asyncio.wait_for(reader.read(4096), timeout=2.0)
            if data:
                message = json.loads(data.decode("utf-8"))
                self.messages_received += 1
                msg_type = message.get("type", "UNKNOWN")
                if msg_type in self.msg_counts:
                    self.msg_counts[msg_type] += 1
                await self.handler_callback(message)
        except Exception:
            # Exceção de timeout ou parsing em conexões incompletas
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def send_message(
        self, host: str, port: int, payload: dict, timeout: float = 1.5
    ) -> bool:
        """Envia mensagem TCP com timeout/deadline explícito (evita bloqueios de rede)."""
        try:
            connect_coro = asyncio.open_connection(host, port)
            reader, writer = await asyncio.wait_for(connect_coro, timeout=timeout)
            data = json.dumps(payload).encode("utf-8")
            writer.write(data)
            await asyncio.wait_for(writer.drain(), timeout=timeout)
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=timeout)
            self.messages_sent += 1
            msg_type = payload.get("type", "UNKNOWN")
            if msg_type in self.msg_counts:
                self.msg_counts[msg_type] += 1
            return True
        except Exception:
            return False
