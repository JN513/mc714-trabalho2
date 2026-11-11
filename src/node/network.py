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
    curta (abre, escreve, fecha). A camada também mantém contadores de tráfego
    usados para medir a complexidade de mensagens dos algoritmos.
    """

    def __init__(self, node_id: int, port: int, handler_callback):
        self.node_id = node_id
        self.port = port
        self.handler_callback = handler_callback
        self.server = None
        # Contadores de tráfego (instrumentação para a análise experimental).
        self.messages_sent = 0
        self.messages_received = 0

    async def start_server(self):
        self.server = await asyncio.start_server(
            self._handle_client, "0.0.0.0", self.port
        )
        logging.info(f"Node {self.node_id} escutando na porta {self.port}")

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        try:
            data = await reader.read(2048)
            if data:
                message = json.loads(data.decode("utf-8"))
                self.messages_received += 1
                await self.handler_callback(message)
        except Exception as e:
            logging.error(f"Erro ao processar mensagem recebida: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def send_message(self, host: str, port: int, payload: dict) -> bool:
        try:
            reader, writer = await asyncio.open_connection(host, port)
            writer.write(json.dumps(payload).encode("utf-8"))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            self.messages_sent += 1
            return True
        except Exception:
            return False
