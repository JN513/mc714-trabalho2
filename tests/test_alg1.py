import sys
import os
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

# Adiciona o caminho de src/node ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/node")))

from alg1_bully import BullyElection


@pytest.mark.asyncio
async def test_bully_highest_id_wins_automatically():
    """Nó com maior ID deve se autodeclarar líder se não houver nós com ID maior."""
    mock_net = AsyncMock()
    peers = {1: ("node1", 5000), 2: ("node2", 5000)}
    bully = BullyElection(node_id=3, peers=peers, network=mock_net)

    await bully.start_election()

    assert bully.is_leader() is True
    assert bully.is_electing is False
    # Deve avisar todos os vizinhos via COORDINATOR
    assert mock_net.send_message.call_count == 2


@pytest.mark.asyncio
async def test_bully_handle_election_from_lower_id():
    """Nó deve responder ANSWER ao receber ELECTION de nó com ID inferior."""
    mock_net = AsyncMock()
    peers = {1: ("node1", 5000), 3: ("node3", 5000)}
    bully = BullyElection(node_id=2, peers=peers, network=mock_net)

    await bully.handle_election_msg(sender_id=1)

    mock_net.send_message.assert_called_with(
        "node1", 5000, {"type": "ANSWER", "sender_id": 2}
    )


@pytest.mark.asyncio
async def test_bully_handle_coordinator_update():
    """Receber COORDINATOR deve atualizar o líder e notificar o detector de falhas."""
    mock_net = AsyncMock()
    mock_hb = MagicMock()
    peers = {1: ("node1", 5000), 3: ("node3", 5000)}
    bully = BullyElection(node_id=2, peers=peers, network=mock_net)

    await bully.handle_coordinator_msg(new_leader_id=3, heartbeat_detector=mock_hb)

    assert bully.leader_id == 3
    assert bully.is_electing is False
    mock_hb.update_heartbeat_timestamp.assert_called_once()
