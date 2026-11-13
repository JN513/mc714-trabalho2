import sys
import os
import unittest
import asyncio
import time
from unittest.mock import AsyncMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/node")))

from alg2_heartbeat import HeartbeatDetector


class TestHeartbeatDetector(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_update_timestamp(self):
        """Atualização do timestamp deve alterar o tempo do último heartbeat."""
        mock_net = AsyncMock()
        hb = HeartbeatDetector(node_id=1, network=mock_net, timeout=3.0)

        old_timestamp = hb.last_heartbeat
        await asyncio.sleep(0.01)
        hb.update_heartbeat_timestamp()

        self.assertGreater(hb.last_heartbeat, old_timestamp)

    async def test_heartbeat_timeout_triggers_election_callback(self):
        """Quando o heartbeat expira, o callback de timeout (eleição) deve ser chamado."""
        mock_net = AsyncMock()
        hb = HeartbeatDetector(node_id=1, network=mock_net, interval=0.05, timeout=0.1)
        hb.last_heartbeat = time.time() - 0.5  # Força timeout estourado

        callback_mock = AsyncMock()
        is_leader_func = lambda: False
        is_electing_func = lambda: False

        checker_task = asyncio.create_task(
            hb.run_checker(is_leader_func, is_electing_func, callback_mock)
        )

        await asyncio.sleep(0.25)
        checker_task.cancel()
        try:
            await checker_task
        except asyncio.CancelledError:
            pass

        callback_mock.assert_called()

    async def test_heartbeat_sender_sends_to_peers_when_leader(self):
        """Líder ativo deve enviar mensagens HEARTBEAT a todos os seus vizinhos."""
        mock_net = AsyncMock()
        hb = HeartbeatDetector(node_id=3, network=mock_net, interval=0.05, timeout=0.2)
        peers = {1: ("node1", 5000), 2: ("node2", 5000)}

        sender_task = asyncio.create_task(
            hb.run_sender(is_leader_func=lambda: True, peers=peers)
        )

        await asyncio.sleep(0.12)
        sender_task.cancel()
        try:
            await sender_task
        except asyncio.CancelledError:
            pass

        self.assertGreaterEqual(mock_net.send_message.call_count, 2)


if __name__ == "__main__":
    unittest.main()
