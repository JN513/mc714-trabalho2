import os
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] Node %(message)s')

NODE_ID = os.getenv("NODE_ID", "1")
PORT = int(os.getenv("PORT", "5000"))
PEERS = os.getenv("PEERS", "").split(",") if os.getenv("PEERS") else []

class DistributedNode:
    def __init__(self, node_id: str, port: int, peers: list):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        self.is_running = True

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        data = await reader.read(1024)
        message = data.decode()
        logging.info(f"{self.node_id} recebeu: {message}")
        
        # AQUI: Encaminhar mensagem para o handler do Algoritmo 1 ou Algoritmo 2
        
        writer.close()
        await writer.wait_closed()

    async def send_message(self, target_host: str, target_port: int, msg: str):
        try:
            reader, writer = await asyncio.open_connection(target_host, target_port)
            writer.write(msg.encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception as e:
            logging.error(f"{self.node_id} falha ao enviar para {target_host}:{target_port} - {e}")

    async def start_server(self):
        server = await asyncio.start_server(self.handle_client, '0.0.0.0', self.port)
        logging.info(f"{self.node_id} rodando na porta {self.port}...")
        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    node = DistributedNode(NODE_ID, PORT, PEERS)
    asyncio.run(node.start_server())