"""
Additional tests — lifecycle, on_result handler, and FastAPI endpoints.
Pushes total coverage above 75%.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "api"))

from orchestrator.orchestrator import RestaurantOrchestrator, Task, TaskResult


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------
class TestLifecycle:
    @pytest.mark.asyncio
    async def test_start_connects_nats_and_redis(self) -> None:
        orch = RestaurantOrchestrator()
        mock_nc = AsyncMock()
        mock_redis = MagicMock()

        with (
            patch("orchestrator.orchestrator.nats.connect", return_value=mock_nc) as mock_nats,
            patch("orchestrator.orchestrator.aioredis.from_url", return_value=mock_redis),
        ):
            await orch.start()

        mock_nats.assert_called_once()
        mock_nc.subscribe.assert_called_once_with("tasks.completed", cb=orch._on_result)
        assert orch._nc is mock_nc

    @pytest.mark.asyncio
    async def test_stop_drains_nats_and_closes_redis(self) -> None:
        orch = RestaurantOrchestrator()
        orch._nc = AsyncMock()
        orch._redis = AsyncMock()
        await orch.stop()
        orch._nc.drain.assert_called_once()
        orch._redis.aclose.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_with_no_connections_does_not_raise(self) -> None:
        orch = RestaurantOrchestrator()
        await orch.stop()  # _nc and _redis are None


# ---------------------------------------------------------------------------
# _on_result handler
# ---------------------------------------------------------------------------
class TestOnResult:
    def _msg(self, data: bytes) -> MagicMock:
        m = MagicMock()
        m.data = data
        return m

    def _result_bytes(self, task_id: str, success: bool = True) -> bytes:
        return json.dumps({
            "task_id": task_id, "success": success,
            "output": {}, "error": "", "agent_id": "a1", "trace_id": "t1",
        }).encode()

    @pytest.mark.asyncio
    async def test_resolves_pending_future(self) -> None:
        orch = RestaurantOrchestrator()
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        orch._pending["abc"] = future
        await orch._on_result(self._msg(self._result_bytes("abc")))
        assert future.done()
        assert future.result().agent_id == "a1"
        assert "abc" not in orch._pending

    @pytest.mark.asyncio
    async def test_ignores_unknown_task_id(self) -> None:
        orch = RestaurantOrchestrator()
        await orch._on_result(self._msg(self._result_bytes("unknown")))

    @pytest.mark.asyncio
    async def test_ignores_malformed_json(self) -> None:
        orch = RestaurantOrchestrator()
        await orch._on_result(self._msg(b"bad json!!!"))

    @pytest.mark.asyncio
    async def test_skips_already_done_future(self) -> None:
        orch = RestaurantOrchestrator()
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        future.cancel()
        orch._pending["done"] = future
        await orch._on_result(self._msg(self._result_bytes("done")))


# ---------------------------------------------------------------------------
# FastAPI endpoint tests
# ---------------------------------------------------------------------------
class TestAPI:
    @pytest.fixture
    def api_client(self):
        """Inject mock orch directly without triggering lifespan (no NATS connect)."""
        import importlib
        import api.main as api_module
        from fastapi.testclient import TestClient

        mock_orch = MagicMock()
        mock_orch.process_order = AsyncMock(return_value={
            "order_id": "order-123",
            "status": "delivered",
            "table_number": 3,
            "total": 700.0,
            "delivered_at": "2026-05-21T10:00:00Z",
            "steps": {},
        })
        mock_orch.get_order_status = AsyncMock(return_value={
            "status": "delivered",
            "order_id": "order-123",
        })
        mock_orch.get_metrics = AsyncMock(return_value={
            "processed": 5, "failed": 1, "pending": 0,
        })

        saved = api_module.orch
        api_module.orch = mock_orch
        # Pass lifespan=None to bypass startup/shutdown (FastAPI TestClient approach)
        c = TestClient(api_module.app, raise_server_exceptions=True)
        yield c, mock_orch
        api_module.orch = saved

    def test_health_ok(self, api_client) -> None:
        c, _ = api_client
        r = c.get("/health")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_create_order_201(self, api_client) -> None:
        c, _ = api_client
        r = c.post("/orders", json={
            "table_number": 3,
            "items": [{"name": "Борщ", "price": 350.0, "quantity": 2}],
        })
        assert r.status_code == 201
        assert r.json()["status"] == "delivered"

    def test_create_order_empty_items_422(self, api_client) -> None:
        c, _ = api_client
        r = c.post("/orders", json={"table_number": 3, "items": []})
        assert r.status_code == 422

    def test_create_order_invalid_table_422(self, api_client) -> None:
        c, _ = api_client
        r = c.post("/orders", json={"table_number": 99, "items": [{"name": "X", "price": 10.0}]})
        assert r.status_code == 422

    def test_get_order_found(self, api_client) -> None:
        c, _ = api_client
        r = c.get("/orders/order-123")
        assert r.status_code == 200
        assert r.json()["status"] == "delivered"

    def test_get_order_not_found(self, api_client) -> None:
        c, mock_orch = api_client
        mock_orch.get_order_status = AsyncMock(return_value=None)
        r = c.get("/orders/missing")
        assert r.status_code == 404

    def test_metrics(self, api_client) -> None:
        c, _ = api_client
        r = c.get("/metrics")
        assert r.status_code == 200
        assert r.json()["processed"] == 5

    def test_create_order_503_when_orch_none(self) -> None:
        import api.main as api_module
        from fastapi.testclient import TestClient
        saved = api_module.orch
        api_module.orch = None
        c = TestClient(api_module.app, raise_server_exceptions=False)
        r = c.post("/orders", json={
            "table_number": 3,
            "items": [{"name": "X", "price": 10.0}],
        })
        assert r.status_code == 503
        api_module.orch = saved
