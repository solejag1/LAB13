"""
Restaurant Order Orchestrator.

Manages the full pipeline:
  validate_order → assign_table → cook_dish → deliver_order

Features:
- Async NATS messaging
- Retry with exponential back-off (max 3 attempts)
- Auction-based agent selection (agents bid on tasks)
- Distributed tracing via OpenTelemetry → Jaeger
- Order state persistence in Redis
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from dataclasses import dataclass, field
from typing import Any

import nats
import redis.asyncio as aioredis
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
MAX_RETRIES = 3
TASK_TIMEOUT = 30  # seconds


# ---------------------------------------------------------------------------
# OpenTelemetry setup
# ---------------------------------------------------------------------------
def init_tracer() -> trace.Tracer:
    """Initialize OTLP tracer pointing at Jaeger."""
    try:
        resource = Resource.create({"service.name": "orchestrator"})
        provider = TracerProvider(resource=resource)
        exporter = OTLPSpanExporter(endpoint=OTLP_ENDPOINT, insecure=True)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        logger.info("OpenTelemetry tracer initialised → %s", OTLP_ENDPOINT)
    except Exception as exc:  # pragma: no cover
        logger.warning("Tracer init failed (no tracing): %s", exc)
    return trace.get_tracer("orchestrator")


tracer = init_tracer()


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------
@dataclass
class Task:
    id: str
    type: str
    payload: dict[str, Any]
    trace_id: str = ""
    retry_count: int = 0


@dataclass
class TaskResult:
    task_id: str
    success: bool
    output: dict[str, Any] = field(default_factory=dict)
    error: str = ""
    agent_id: str = ""
    trace_id: str = ""


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class RestaurantOrchestrator:
    """Central coordinator for restaurant order processing."""

    def __init__(self) -> None:
        self._nc: nats.NATS | None = None
        self._redis: aioredis.Redis | None = None
        self._pending: dict[str, asyncio.Future[TaskResult]] = {}
        self._processed: int = 0
        self._failed: int = 0

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        """Connect to NATS and Redis, start result listener."""
        self._nc = await nats.connect(
            NATS_URL,
            reconnect_time_wait=2,
            max_reconnect_attempts=-1,
        )
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        await self._nc.subscribe("tasks.completed", cb=self._on_result)
        logger.info("Orchestrator connected — NATS=%s  Redis=%s", NATS_URL, REDIS_URL)

    async def stop(self) -> None:
        """Graceful shutdown."""
        if self._nc:
            await self._nc.drain()
        if self._redis:
            await self._redis.aclose()
        logger.info(
            "Orchestrator stopped. processed=%d  failed=%d",
            self._processed,
            self._failed,
        )

    # ------------------------------------------------------------------
    # Result listener
    # ------------------------------------------------------------------
    async def _on_result(self, msg: nats.aio.client.Msg) -> None:
        """Handle task results published by agents."""
        try:
            data = json.loads(msg.data.decode())
            result = TaskResult(**data)
        except Exception as exc:
            logger.error("Malformed result message: %s", exc)
            return

        future = self._pending.pop(result.task_id, None)
        if future and not future.done():
            future.set_result(result)

    # ------------------------------------------------------------------
    # Core send / retry
    # ------------------------------------------------------------------
    async def send_task(self, task: Task, timeout: int = TASK_TIMEOUT) -> TaskResult:
        """
        Send a task to agents and await the result.

        Retries up to MAX_RETRIES times on failure with exponential back-off.
        """
        assert self._nc is not None

        for attempt in range(1, MAX_RETRIES + 1):
            task.retry_count = attempt - 1
            task.id = str(uuid.uuid4())

            future: asyncio.Future[TaskResult] = asyncio.get_event_loop().create_future()
            self._pending[task.id] = future

            payload = json.dumps(
                {
                    "id": task.id,
                    "type": task.type,
                    "payload": task.payload,
                    "trace_id": task.trace_id,
                    "retry_count": task.retry_count,
                }
            ).encode()
            await self._nc.publish("tasks.process", payload)
            logger.info(
                "Task sent  type=%s  id=%s  attempt=%d/%d",
                task.type,
                task.id,
                attempt,
                MAX_RETRIES,
            )

            try:
                result = await asyncio.wait_for(future, timeout=timeout)
                if result.success:
                    self._processed += 1
                    return result
                # Agent returned failure — retry
                logger.warning(
                    "Task failed (agent error) type=%s id=%s error=%s — retrying",
                    task.type,
                    task.id,
                    result.error,
                )
            except TimeoutError:
                self._pending.pop(task.id, None)
                logger.warning(
                    "Task timeout  type=%s  attempt=%d/%d",
                    task.type,
                    attempt,
                    MAX_RETRIES,
                )

            # Exponential back-off before next attempt
            await asyncio.sleep(2 ** (attempt - 1))

        self._failed += 1
        raise RuntimeError(f"Task {task.type} failed after {MAX_RETRIES} attempts")

    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------
    async def process_order(
        self, order: dict[str, Any], step_timeout: int = TASK_TIMEOUT
    ) -> dict[str, Any]:
        """
        Full pipeline: validate → assign_table → cook → deliver.

        Each step's output feeds into the next step's payload.
        ``step_timeout`` controls per-step timeout (default 30s; lower in tests).
        """
        order_id = order.get("order_id") or str(uuid.uuid4())
        order["order_id"] = order_id
        trace_id = str(uuid.uuid4())

        with tracer.start_as_current_span("process_order") as span:
            span.set_attribute("order.id", order_id)

            # Save initial status
            await self._save_status(order_id, "received", order)

            try:
                # Step 1: Validate order
                logger.info("[%s] Step 1 — validate_order", order_id)
                r1 = await self.send_task(
                    Task(id="", type="validate_order", payload=order, trace_id=trace_id),
                    timeout=step_timeout,
                )
                await self._save_status(order_id, "validated", r1.output)

                # Step 2: Assign table
                logger.info("[%s] Step 2 — assign_table", order_id)
                r2 = await self.send_task(
                    Task(id="", type="assign_table", payload=r1.output, trace_id=trace_id),
                    timeout=step_timeout,
                )
                await self._save_status(order_id, "table_assigned", r2.output)

                # Step 3: Cook dishes
                logger.info("[%s] Step 3 — cook_dish", order_id)
                cook_payload = {**r1.output, **r2.output}
                r3 = await self.send_task(
                    Task(id="", type="cook_dish", payload=cook_payload, trace_id=trace_id),
                    timeout=step_timeout,
                )
                await self._save_status(order_id, "cooking", r3.output)

                # Step 4: Deliver to table
                logger.info("[%s] Step 4 — deliver_order", order_id)
                delivery_payload = {**r3.output, "table_number": r2.output.get("table_number")}
                r4 = await self.send_task(
                    Task(id="", type="deliver_order", payload=delivery_payload, trace_id=trace_id),
                    timeout=step_timeout,
                )
                await self._save_status(order_id, "delivered", r4.output)

                logger.info("[%s] Order pipeline complete ✓", order_id)
                return {
                    "order_id": order_id,
                    "status": "delivered",
                    "table_number": r2.output.get("table_number"),
                    "total": r1.output.get("total"),
                    "delivered_at": r4.output.get("delivered_at"),
                    "steps": {
                        "validate": r1.output,
                        "table": r2.output,
                        "kitchen": r3.output,
                        "delivery": r4.output,
                    },
                }

            except Exception as exc:
                await self._save_status(order_id, "failed", {"error": str(exc)})
                span.record_exception(exc)
                logger.error("[%s] Pipeline failed: %s", order_id, exc)
                raise

    # ------------------------------------------------------------------
    # Redis state helpers
    # ------------------------------------------------------------------
    async def _save_status(
        self, order_id: str, status: str, data: dict[str, Any]
    ) -> None:
        if not self._redis:
            return
        key = f"order:status:{order_id}"
        serialisable = {k: str(v) for k, v in data.items()}
        await self._redis.hset(key, mapping={"status": status, **serialisable})
        await self._redis.expire(key, 86400)
        # Append to order event log
        await self._redis.rpush(
            f"order:log:{order_id}",
            json.dumps({"status": status, "data": data}),
        )
        await self._redis.expire(f"order:log:{order_id}", 86400)

    async def get_order_status(self, order_id: str) -> dict[str, Any] | None:
        """Retrieve order status from Redis."""
        if not self._redis:
            return None
        data = await self._redis.hgetall(f"order:status:{order_id}")
        return data or None

    async def get_metrics(self) -> dict[str, Any]:
        """Return orchestrator metrics."""
        return {
            "processed": self._processed,
            "failed": self._failed,
            "pending": len(self._pending),
        }
