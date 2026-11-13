import sys
import os
import unittest
from unittest.mock import AsyncMock, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/node")))

from alg1_bully import BullyElection


class TestBullyElection(unittest.IsolatedAsyncioTestCase):
    async def test_bully_highest_id_wins_and_announces(self):
        """Nó com maior ID deve se autodeclarar líder e enviar COORDINATOR a todos."""
        mock_net = AsyncMock()
        peers = {1: ("node1", 5000), 2: ("node2", 5000)}
        bully = BullyElection(node_id=3, peers=peers, network=mock_net)

        await bully.start_election()

        self.assertTrue(bully.is_leader())
        self.assertFalse(bully.is_electing)
        self.assertEqual(bully.leader_id, 3)
        # Deve avisar todos os nós via COORDINATOR
        self.assertEqual(mock_net.send_message.call_count, 2)

    async def test_bully_handle_election_from_lower_id(self):
        """Nó com ID superior responde ANSWER para nó com ID inferior."""
        mock_net = AsyncMock()
        peers = {1: ("node1", 5000), 3: ("node3", 5000)}
        bully = BullyElection(node_id=2, peers=peers, network=mock_net)

        await bully.handle_election_msg(sender_id=1)

        mock_net.send_message.assert_called_with(
            "node1", 5000, {"type": "ANSWER", "sender_id": 2}
        )

    async def test_bully_handle_coordinator_update(self):
        """Receber mensagem COORDINATOR atualiza o líder e desativa estado de eleição."""
        mock_net = AsyncMock()
        mock_hb = MagicMock()
        peers = {1: ("node1", 5000), 3: ("node3", 5000)}
        bully = BullyElection(node_id=2, peers=peers, network=mock_net)

        await bully.handle_coordinator_msg(new_leader_id=3, heartbeat_detector=mock_hb)

        self.assertEqual(bully.leader_id, 3)
        self.assertFalse(bully.is_electing)
        self.assertFalse(bully.is_leader())
        mock_hb.update_heartbeat_timestamp.assert_called_once()

    async def test_bully_crash_recovery_prevents_split_brain(self):
        """Crash-Recovery: nó 2 é líder provisório, nó 3 recupera e assume sem split-brain."""
        mock_net = AsyncMock()
        mock_hb = MagicMock()
        peers_node2 = {1: ("node1", 5000), 3: ("node3", 5000)}
        bully2 = BullyElection(node_id=2, peers=peers_node2, network=mock_net)
        bully2.leader_id = 2  # Nó 2 estava como líder

        peers_node3 = {1: ("node1", 5000), 2: ("node2", 5000)}
        bully3 = BullyElection(node_id=3, peers=peers_node3, network=mock_net)

        # Nó 3 reinicia e convoca eleição
        await bully3.start_election()
        self.assertTrue(bully3.is_leader())

        # Nó 2 recebe anúncio do Nó 3
        bully2.abdicate_if_higher_leader(3)
        await bully2.handle_coordinator_msg(3, mock_hb)

        # Invariante de Segurança (Safety): apenas 1 líder no cluster
        self.assertFalse(bully2.is_leader())
        self.assertTrue(bully3.is_leader())
        self.assertEqual(bully2.leader_id, 3)
        self.assertEqual(bully3.leader_id, 3)


if __name__ == "__main__":
    unittest.main()
