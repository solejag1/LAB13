"""
Tests for AutoScaler — scale up/down decisions, metrics, Docker interaction.
All Docker and Redis calls are mocked.
"""
from __future__ import annotations

import asyncio
import sys
import os
import importlib.util
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

# Load scaler/main.py explicitly to avoid collision with orchestrator/main.py
_scaler_path = os.path.join(os.path.dirname(__file__), "..", "scaler", "main.py")
_spec = importlib.util.spec_from_file_location("scaler_main", _scaler_path)
scaler_main = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(scaler_main)


def _make_scaler() -> scaler_main.AutoScaler:
    scaler = scaler_main.AutoScaler.__new__(scaler_main.AutoScaler)
    scaler._docker = MagicMock()
    scaler._redis = AsyncMock()
    scaler._dynamic_count = 0
    return scaler


class TestGetPendingTasks:
    @pytest.mark.asyncio
    async def test_returns_redis_value(self) -> None:
        redis = AsyncMock()
        redis.get = AsyncMock(return_value="5")
        result = await scaler_main.get_pending_tasks(redis)
        assert result == 5

    @pytest.mark.asyncio
    async def test_returns_zero_when_key_missing(self) -> None:
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        result = await scaler_main.get_pending_tasks(redis)
        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_on_error(self) -> None:
        redis = AsyncMock()
        redis.get = AsyncMock(side_effect=Exception("connection lost"))
        result = await scaler_main.get_pending_tasks(redis)
        assert result == 0


class TestScaleUp:
    @pytest.mark.asyncio
    async def test_spawns_container_when_pending_above_threshold(self) -> None:
        scaler = _make_scaler()
        scaler._redis.get = AsyncMock(return_value=str(scaler_main.SCALE_UP_THRESHOLD))
        scaler._redis.hset = AsyncMock()
        scaler._redis.rpush = AsyncMock()

        # list_dynamic_containers returns empty (0 dynamic so far)
        with (
            patch.object(scaler_main, "list_dynamic_containers", return_value=[]),
            patch.object(scaler_main, "get_nats_queue_depth", new=AsyncMock(return_value=0)),
            patch.object(scaler_main, "spawn_kitchen_agent", return_value="lab13_kitchen_dynamic_1") as mock_spawn,
        ):
            await scaler.tick()

        mock_spawn.assert_called_once()
        assert scaler._dynamic_count == 1

    @pytest.mark.asyncio
    async def test_does_not_exceed_max_instances(self) -> None:
        scaler = _make_scaler()
        scaler._redis.get = AsyncMock(return_value="99")
        scaler._redis.hset = AsyncMock()
        scaler._redis.rpush = AsyncMock()

        # Already at MAX_INSTANCES - MIN_INSTANCES dynamic containers
        max_dynamic = scaler_main.MAX_INSTANCES - scaler_main.MIN_INSTANCES
        fake_containers = [MagicMock() for _ in range(max_dynamic)]

        with (
            patch.object(scaler_main, "list_dynamic_containers", return_value=fake_containers),
            patch.object(scaler_main, "get_nats_queue_depth", new=AsyncMock(return_value=0)),
            patch.object(scaler_main, "spawn_kitchen_agent") as mock_spawn,
        ):
            await scaler.tick()

        mock_spawn.assert_not_called()


class TestScaleDown:
    @pytest.mark.asyncio
    async def test_removes_container_when_pending_below_threshold(self) -> None:
        scaler = _make_scaler()
        scaler._dynamic_count = 2
        scaler._redis.get = AsyncMock(return_value="0")
        scaler._redis.hset = AsyncMock()
        scaler._redis.rpush = AsyncMock()

        fake_container = MagicMock()
        fake_container.name = "lab13_kitchen_dynamic_1_ts"

        with (
            patch.object(scaler_main, "list_dynamic_containers", return_value=[fake_container]),
            patch.object(scaler_main, "get_nats_queue_depth", new=AsyncMock(return_value=0)),
            patch.object(scaler_main, "remove_kitchen_agent", return_value="lab13_kitchen_dynamic_1_ts") as mock_remove,
        ):
            await scaler.tick()

        mock_remove.assert_called_once()

    @pytest.mark.asyncio
    async def test_does_not_remove_when_no_dynamic_containers(self) -> None:
        scaler = _make_scaler()
        scaler._redis.get = AsyncMock(return_value="0")
        scaler._redis.hset = AsyncMock()

        with (
            patch.object(scaler_main, "list_dynamic_containers", return_value=[]),
            patch.object(scaler_main, "get_nats_queue_depth", new=AsyncMock(return_value=0)),
            patch.object(scaler_main, "remove_kitchen_agent") as mock_remove,
        ):
            await scaler.tick()

        mock_remove.assert_not_called()


class TestNoScaling:
    @pytest.mark.asyncio
    async def test_no_action_when_pending_between_thresholds(self) -> None:
        scaler = _make_scaler()
        # pending = 2: above SCALE_DOWN (1) but below SCALE_UP (3)
        scaler._redis.get = AsyncMock(return_value="2")
        scaler._redis.hset = AsyncMock()

        with (
            patch.object(scaler_main, "list_dynamic_containers", return_value=[]),
            patch.object(scaler_main, "get_nats_queue_depth", new=AsyncMock(return_value=0)),
            patch.object(scaler_main, "spawn_kitchen_agent") as mock_spawn,
            patch.object(scaler_main, "remove_kitchen_agent") as mock_remove,
        ):
            await scaler.tick()

        mock_spawn.assert_not_called()
        mock_remove.assert_not_called()


class TestDockerHelpers:
    def test_list_dynamic_containers_filters_by_prefix(self) -> None:
        mock_client = MagicMock()
        c1 = MagicMock()
        c1.name = "lab13_kitchen_dynamic_1_ts"
        c2 = MagicMock()
        c2.name = "lab13_kitchen_agent"
        c3 = MagicMock()
        c3.name = "lab13_api"
        mock_client.containers.list.return_value = [c1, c2, c3]

        result = scaler_main.list_dynamic_containers(mock_client)
        assert result == [c1]
        assert len(result) == 1
