import asyncio
import json
import logging

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)


class NetworkManager:
    def __init__(self, node_id: int, port: int, handler_callback):
        self.node_id = node_id
        self.port = port
        self.handler_callback = handler_callback
        self.server = None

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
            return True
        except Exception:
            return False
