import sys
import os
import pytest
import asyncio
import time
from unittest.mock import AsyncMock, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../src/node")))

from alg2_heartbeat import HeartbeatDetector


@pytest.mark.asyncio
async def test_heartbeat_update_timestamp():
    """Atualização do timestamp deve alterar o tempo do último heartbeat."""
    mock_net = AsyncMock()
    hb = HeartbeatDetector(node_id=1, network=mock_net, timeout=3.0)

    old_timestamp = hb.last_heartbeat
    await asyncio.sleep(0.01)
    hb.update_heartbeat_timestamp()

    assert hb.last_heartbeat > old_timestamp


@pytest.mark.asyncio
async def test_heartbeat_timeout_triggers_election_callback():
    """Quando o heartbeat expira, o callback de timeout (eleição) deve ser chamado."""
    mock_net = AsyncMock()
    hb = HeartbeatDetector(node_id=1, network=mock_net, timeout=0.1)
    # Força timestamp antigo para simular timeout estourado
    hb.last_heartbeat = time.time() - 0.5

    callback_mock = AsyncMock()
    is_leader_func = lambda: False
    is_electing_func = lambda: False

    checker_task = asyncio.create_task(
        hb.run_checker(is_leader_func, is_electing_func, callback_mock)
    )

    await asyncio.sleep(0.6)
    checker_task.cancel()

    callback_mock.assert_called()
