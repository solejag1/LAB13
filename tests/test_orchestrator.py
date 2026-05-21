"""
Orchestrator unit tests.
Uses AsyncMock to mock NATS/Redis — runs without infrastructure.
"""
from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from orchestrator.orchestrator import RestaurantOrchestrator, Task, TaskResult


def _make_orch() -> RestaurantOrchestrator:
    return RestaurantOrchestrator()


async def _inject_result(orch: RestaurantOrchestrator, task_id: str, result: TaskResult) -> None:
    await asyncio.sleep(0.05)
    future = orch._pending.get(task_id)
    if future and not future.done():
        future.set_result(result)


class TestSendTask:
    @pytest.mark.asyncio
    async def test_send_task_success_returns_result(self) -> None:
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()

        async def fake_publish(subject: str, data: bytes) -> None:
            if subject == "tasks.bid":
                return  # auction phase — no bids → fallback broadcast
            task_data = json.loads(data)
            await _inject_result(
                orch, task_data["id"],
                TaskResult(task_id=task_data["id"], success=True, output={"status": "done"}),
            )

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
        orch._nc.publish = AsyncMock()

        task = Task(id="", type="cook_dish", payload={})

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
            if subject == "tasks.bid":
                return
            task_data = json.loads(data)
            await _inject_result(
                orch, task_data["id"],
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
            if subject == "tasks.bid":
                return
            call_count += 1
            task_data = json.loads(data)
            success = call_count >= 3
            await _inject_result(
                orch, task_data["id"],
                TaskResult(task_id=task_data["id"], success=success,
                           error="" if success else "agent error"),
            )

        orch._nc.publish = fake_pub

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await orch.send_task(Task(id="", type="cook_dish", payload={}), timeout=5)

        assert result.success is True
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_auction_winner_gets_direct_task(self) -> None:
        """Verify that when a bid is received, task goes to tasks.direct.{agent_id}."""
        orch = _make_orch()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()
        published_subjects = []

        async def fake_pub(subject: str, data: bytes) -> None:
            published_subjects.append(subject)
            if subject == "tasks.bid":
                # Simulate an agent bidding
                task_data = json.loads(data)
                from orchestrator.orchestrator import Bid
                bid = Bid(
                    task_id=task_data["id"],
                    agent_id="kitchen-agent-1",
                    cost=1.0,
                    task_type=task_data["type"],
                )
                orch._bids[task_data["id"]] = [bid]
                return
            if subject.startswith("tasks.direct."):
                task_data = json.loads(data)
                await _inject_result(
                    orch, task_data["id"],
                    TaskResult(task_id=task_data["id"], success=True),
                )

        orch._nc.publish = fake_pub

        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await orch.send_task(
                Task(id="", type="cook_dish", payload={}), timeout=5
            )

        assert result.success is True
        assert any(s.startswith("tasks.direct.") for s in published_subjects)


class TestOnBid:
    @pytest.mark.asyncio
    async def test_on_bid_stores_bid(self) -> None:
        orch = _make_orch()
        task_id = "test-bid-task"
        orch._bids[task_id] = []

        msg = AsyncMock()
        msg.data = json.dumps({
            "task_id": task_id,
            "agent_id": "kitchen-1",
            "cost": 1.5,
            "task_type": "cook_dish",
        }).encode()

        await orch._on_bid(msg)

        assert len(orch._bids[task_id]) == 1
        assert orch._bids[task_id][0].agent_id == "kitchen-1"
        assert orch._bids[task_id][0].cost == 1.5

    @pytest.mark.asyncio
    async def test_on_bid_ignores_malformed(self) -> None:
        orch = _make_orch()
        msg = AsyncMock()
        msg.data = b"bad json"
        await orch._on_bid(msg)  # should not raise


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
            {"order_id": "o1", "items": [], "status": "ready"},
            {"order_id": "o1", "table_number": 3, "status": "delivered", "delivered_at": "2026-05-21T10:00:00"},
            {"order_id": "o1", "recommendation": "Рекомендуем десерт!", "status": "analyzed"},
        ]

        async def fake_pub(subject: str, data: bytes) -> None:
            nonlocal step
            if subject == "tasks.bid":
                return
            task_data = json.loads(data)
            idx = min(step, len(pipeline_outputs) - 1)
            await _inject_result(
                orch, task_data["id"],
                TaskResult(task_id=task_data["id"], success=True, output=pipeline_outputs[idx]),
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

    @pytest.mark.asyncio
    async def test_process_order_generates_order_id_if_missing(self) -> None:
        orch = self._make_mock_orch()
        step = 0

        async def fake_pub(subject: str, data: bytes) -> None:
            nonlocal step
            if subject == "tasks.bid":
                return
            task_data = json.loads(data)
            outputs = [
                {"order_id": "gen", "table_number": 1, "items": [], "total": 100.0},
                {"order_id": "gen", "table_number": 1, "status": "assigned"},
                {"order_id": "gen", "items": [], "status": "ready"},
                {"order_id": "gen", "table_number": 1, "status": "delivered", "delivered_at": "T"},
                {"order_id": "gen", "recommendation": "ok", "status": "analyzed"},
            ]
            await _inject_result(
                orch, task_data["id"],
                TaskResult(task_id=task_data["id"], success=True, output=outputs[step]),
            )
            step += 1

        orch._nc.publish = fake_pub

        result = await orch.process_order({"table_number": 2, "items": []})
        assert result["order_id"] != ""

    @pytest.mark.asyncio
    async def test_process_order_saves_failed_status_on_error(self) -> None:
        orch = self._make_mock_orch()
        orch._nc.publish = AsyncMock()

        with pytest.raises(RuntimeError):
            with patch("asyncio.sleep", new_callable=AsyncMock):
                await orch.process_order(
                    {"order_id": "fail-test", "table_number": 1, "items": []},
                    step_timeout=0.05,
                )

        calls = [str(c) for c in orch._redis.hset.call_args_list]
        assert any("failed" in s for s in calls)


class TestGetOrderStatus:
    @pytest.mark.asyncio
    async def test_returns_redis_data(self) -> None:
        orch = _make_orch()
        orch._redis = AsyncMock()
        orch._redis.hgetall = AsyncMock(return_value={"status": "delivered"})
        result = await orch.get_order_status("x")
        assert result["status"] == "delivered"

    @pytest.mark.asyncio
    async def test_not_found_returns_none(self) -> None:
        orch = _make_orch()
        orch._redis = AsyncMock()
        orch._redis.hgetall = AsyncMock(return_value={})
        result = await orch.get_order_status("nope")
        assert result is None

    @pytest.mark.asyncio
    async def test_get_metrics(self) -> None:
        orch = _make_orch()
        orch._processed = 7
        orch._failed = 1
        m = await orch.get_metrics()
        assert m["processed"] == 7
        assert m["failed"] == 1
