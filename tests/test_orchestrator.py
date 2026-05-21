"""
Orchestrator unit tests.

Uses AsyncMock to mock NATS and Redis so tests run without infrastructure.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

import sys
import os


from orchestrator.orchestrator import RestaurantOrchestrator, Task, TaskResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_orch() -> RestaurantOrchestrator:
    return RestaurantOrchestrator()


async def _inject_result(orch: RestaurantOrchestrator, task_id: str, result: TaskResult) -> None:
    """Simulate an agent publishing a result back to the orchestrator."""
    await asyncio.sleep(0.05)
    future = orch._pending.get(task_id)
    if future and not future.done():
        future.set_result(result)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestSendTask:
    @pytest.mark.asyncio
    async def test_send_task_success_returns_result(self) -> None:
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()

        async def fake_publish(subject: str, data: bytes) -> None:
            task_data = json.loads(data)
            result = TaskResult(
                task_id=task_data["id"],
                success=True,
                output={"status": "done"},
            )
            await _inject_result(orch, task_data["id"], result)

        orch._nc.publish = fake_publish

        task = Task(id="", type="validate_order", payload={"order_id": "x"})
        result = await orch.send_task(task, timeout=5)

        assert result.success is True
        assert result.output["status"] == "done"

    @pytest.mark.asyncio
    async def test_send_task_timeout_retries_and_raises(self) -> None:
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()
        # Never respond — force timeout
        orch._nc.publish = AsyncMock()

        task = Task(id="", type="cook_dish", payload={})

        # Patch sleep to avoid real 1s+2s backoff in tests
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(RuntimeError, match="failed after"):
                await orch.send_task(task, timeout=0.05)

        assert orch._failed == 1

    @pytest.mark.asyncio
    async def test_send_task_increments_processed_on_success(self) -> None:
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()

        async def fake_pub(subject: str, data: bytes) -> None:
            task_data = json.loads(data)
            await _inject_result(
                orch,
                task_data["id"],
                TaskResult(task_id=task_data["id"], success=True),
            )

        orch._nc.publish = fake_pub

        await orch.send_task(Task(id="", type="assign_table", payload={}), timeout=5)
        assert orch._processed == 1

    @pytest.mark.asyncio
    async def test_send_task_retries_on_agent_failure(self) -> None:
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()
        call_count = 0

        async def fake_pub(subject: str, data: bytes) -> None:
            nonlocal call_count
            call_count += 1
            task_data = json.loads(data)
            # Fail first 2, succeed on 3rd
            success = call_count >= 3
            await _inject_result(
                orch,
                task_data["id"],
                TaskResult(task_id=task_data["id"], success=success, error="" if success else "agent error"),
            )

        orch._nc.publish = fake_pub

        result = await orch.send_task(Task(id="", type="cook_dish", payload={}), timeout=5)
        assert result.success is True
        assert call_count == 3


class TestProcessOrder:
    def _make_mock_orch(self) -> RestaurantOrchestrator:
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()
        orch._redis.hset = AsyncMock()
        orch._redis.expire = AsyncMock()
        orch._redis.rpush = AsyncMock()
        return orch

    @pytest.mark.asyncio
    async def test_process_order_full_pipeline_success(self) -> None:
        orch = self._make_mock_orch()
        step = 0

        pipeline_outputs = [
            {"order_id": "o1", "table_number": 3, "items": [], "total": 700.0, "status": "validated"},
            {"order_id": "o1", "table_number": 3, "status": "assigned"},
            {"order_id": "o1", "items": [], "status": "ready", "cooked_by": "kitchen-1"},
            {"order_id": "o1", "table_number": 3, "status": "delivered", "delivered_at": "2026-05-21T10:00:00"},
        ]

        async def fake_pub(subject: str, data: bytes) -> None:
            nonlocal step
            task_data = json.loads(data)
            await _inject_result(
                orch,
                task_data["id"],
                TaskResult(task_id=task_data["id"], success=True, output=pipeline_outputs[step]),
            )
            step += 1

        orch._nc.publish = fake_pub

        result = await orch.process_order({
            "order_id": "o1",
            "table_number": 3,
            "items": [{"name": "Борщ", "price": 350.0, "quantity": 2}],
        })

        assert result["status"] == "delivered"
        assert result["order_id"] == "o1"
        assert result["table_number"] == 3

    @pytest.mark.asyncio
    async def test_process_order_generates_order_id_if_missing(self) -> None:
        orch = self._make_mock_orch()
        step = 0

        async def fake_pub(subject: str, data: bytes) -> None:
            nonlocal step
            task_data = json.loads(data)
            outputs = [
                {"order_id": task_data["payload"].get("order_id", "gen"), "table_number": 1, "items": [], "total": 100.0},
                {"order_id": "gen", "table_number": 1, "status": "assigned"},
                {"order_id": "gen", "items": [], "status": "ready"},
                {"order_id": "gen", "table_number": 1, "status": "delivered", "delivered_at": "T"},
            ]
            await _inject_result(
                orch,
                task_data["id"],
                TaskResult(task_id=task_data["id"], success=True, output=outputs[step]),
            )
            step += 1

        orch._nc.publish = fake_pub

        result = await orch.process_order({"table_number": 2, "items": []})
        assert "order_id" in result
        assert result["order_id"] != ""

    @pytest.mark.asyncio
    async def test_process_order_saves_failed_status_on_error(self) -> None:
        orch = self._make_mock_orch()
        # Never respond → timeout → failure
        orch._nc.publish = AsyncMock()

        with pytest.raises(RuntimeError):
            # Use very short step_timeout so retries are fast (0.1s × 3 attempts)
            await orch.process_order(
                {"order_id": "fail-test", "table_number": 1, "items": []},
                step_timeout=0.1,
            )

        # _save_status should have been called with "failed"
        calls = orch._redis.hset.call_args_list
        statuses = [str(call) for call in calls]
        assert any("failed" in s for s in statuses)


class TestGetOrderStatus:
    @pytest.mark.asyncio
    async def test_get_order_status_returns_redis_data(self) -> None:
        orch = _make_orch()
        orch._redis = AsyncMock()
        orch._redis.hgetall = AsyncMock(return_value={"status": "delivered", "order_id": "x"})

        result = await orch.get_order_status("x")
        assert result is not None
        assert result["status"] == "delivered"

    @pytest.mark.asyncio
    async def test_get_order_status_not_found_returns_none(self) -> None:
        orch = _make_orch()
        orch._redis = AsyncMock()
        orch._redis.hgetall = AsyncMock(return_value={})

        result = await orch.get_order_status("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_metrics_returns_counters(self) -> None:
        orch = _make_orch()
        orch._processed = 5
        orch._failed = 2

        metrics = await orch.get_metrics()
        assert metrics["processed"] == 5
        assert metrics["failed"] == 2
        assert "pending" in metrics
