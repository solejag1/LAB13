"""
Auto-Scaler — Задание 5: Динамическое масштабирование.

Мониторит количество pending-задач через Redis (ключ orchestrator:pending)
и длину очереди NATS-subject tasks.process через NATS HTTP monitoring API.

Алгоритм:
  - Каждые POLL_INTERVAL секунд считывает pending_tasks
  - Если pending_tasks > SCALE_UP_THRESHOLD → запускает новый kitchen_agent контейнер
  - Если pending_tasks < SCALE_DOWN_THRESHOLD и лишних контейнеров > 0 → останавливает один
  - Максимум MAX_INSTANCES экземпляров, минимум MIN_INSTANCES

Использует docker SDK: github.com/docker/docker/client (через docker-py в Python).
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

import docker
import redis.asyncio as aioredis
import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("scaler")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
NATS_MONITOR_URL = os.getenv("NATS_MONITOR_URL", "http://nats:8222")
DOCKER_IMAGE = os.getenv("KITCHEN_AGENT_IMAGE", "lab13-kitchen_agent")
NATS_URL_FOR_AGENT = os.getenv("NATS_URL", "nats://nats:4222")
REDIS_URL_FOR_AGENT = os.getenv("REDIS_URL", "redis://redis:6379")
OTLP_ENDPOINT = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4317")

POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))       # seconds between checks
SCALE_UP_THRESHOLD = int(os.getenv("SCALE_UP_THRESHOLD", "3"))   # pending tasks → add instance
SCALE_DOWN_THRESHOLD = int(os.getenv("SCALE_DOWN_THRESHOLD", "1"))  # pending tasks → remove instance
MIN_INSTANCES = int(os.getenv("MIN_INSTANCES", "1"))
MAX_INSTANCES = int(os.getenv("MAX_INSTANCES", "5"))

CONTAINER_PREFIX = "lab13_kitchen_dynamic_"


# ---------------------------------------------------------------------------
# Docker helpers
# ---------------------------------------------------------------------------
def get_docker_client() -> docker.DockerClient:
    return docker.from_env()


def list_dynamic_containers(client: docker.DockerClient) -> list[Any]:
    """Return all dynamically spawned kitchen_agent containers."""
    return [
        c for c in client.containers.list(all=True)
        if c.name.startswith(CONTAINER_PREFIX)
    ]


def count_all_kitchen_containers(client: docker.DockerClient) -> int:
    """Count static + dynamic running kitchen_agent containers."""
    running = client.containers.list(filters={"status": "running"})
    count = 0
    for c in running:
        tags = c.image.tags if c.image.tags else []
        if "kitchen_agent" in c.name or any("kitchen" in t for t in tags):
            count += 1
    return count


def spawn_kitchen_agent(client: docker.DockerClient, instance_num: int) -> str:
    """Start a new kitchen_agent container via Docker SDK."""
    name = f"{CONTAINER_PREFIX}{instance_num}_{int(time.time())}"
    container = client.containers.run(
        image=DOCKER_IMAGE,
        name=name,
        detach=True,
        environment={
            "NATS_URL": NATS_URL_FOR_AGENT,
            "REDIS_URL": REDIS_URL_FOR_AGENT,
            "OTEL_EXPORTER_OTLP_ENDPOINT": OTLP_ENDPOINT,
            "AGENT_ID": f"kitchen-agent-dynamic-{instance_num}",
        },
        network="lab13_default",
        restart_policy={"Name": "unless-stopped"},
    )
    logger.info("Spawned container  name=%s  id=%s", name, container.short_id)
    return name


def remove_kitchen_agent(client: docker.DockerClient) -> str | None:
    """Stop and remove one dynamic kitchen_agent container."""
    dynamic = list_dynamic_containers(client)
    if not dynamic:
        return None
    # Remove the most recently created one
    target = sorted(dynamic, key=lambda c: c.attrs["Created"])[-1]
    target.stop(timeout=5)
    target.remove()
    logger.info("Removed container  name=%s", target.name)
    return target.name


# ---------------------------------------------------------------------------
# Metrics collection
# ---------------------------------------------------------------------------
async def get_pending_tasks(redis_client: aioredis.Redis) -> int:
    """Read pending task count written by orchestrator to Redis."""
    try:
        val = await redis_client.get("orchestrator:pending")
        return int(val) if val else 0
    except Exception as exc:
        logger.warning("Redis read error: %s", exc)
        return 0


async def get_nats_queue_depth() -> int:
    """
    Query NATS HTTP monitoring API for tasks.process subject message count.
    Falls back to 0 on error.
    """
    try:
        async with httpx.AsyncClient(timeout=3) as client:
            resp = await client.get(f"{NATS_MONITOR_URL}/subsz", params={"limit": 256})
            if resp.status_code != 200:
                return 0
            data = resp.json()
            for sub in data.get("subscriptions", []):
                if sub.get("subject") == "tasks.process":
                    return sub.get("msgs_waiting", 0)
    except Exception as exc:
        logger.debug("NATS monitor unavailable: %s", exc)
    return 0


# ---------------------------------------------------------------------------
# Scaler loop
# ---------------------------------------------------------------------------
class AutoScaler:
    def __init__(self) -> None:
        self._docker = get_docker_client()
        self._redis: aioredis.Redis | None = None
        self._dynamic_count = 0  # number of containers WE spawned this session

    async def start(self) -> None:
        self._redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        logger.info(
            "AutoScaler started  poll=%ds  scale_up>%d  scale_down<%d  min=%d  max=%d",
            POLL_INTERVAL, SCALE_UP_THRESHOLD, SCALE_DOWN_THRESHOLD,
            MIN_INSTANCES, MAX_INSTANCES,
        )

    async def stop(self) -> None:
        if self._redis:
            await self._redis.aclose()

    async def _get_metrics(self) -> dict[str, int]:
        pending_redis = await get_pending_tasks(self._redis)
        pending_nats = await get_nats_queue_depth()
        # Use max of both sources for a conservative estimate
        pending = max(pending_redis, pending_nats)
        dynamic = len(list_dynamic_containers(self._docker))
        return {"pending": pending, "dynamic_containers": dynamic}

    async def tick(self) -> None:
        """One scaling decision cycle."""
        try:
            metrics = await self._get_metrics()
            pending = metrics["pending"]
            dynamic = metrics["dynamic_containers"]
            total_dynamic_possible = MAX_INSTANCES - MIN_INSTANCES

            logger.info(
                "Scaling check  pending=%d  dynamic_containers=%d",
                pending, dynamic,
            )

            # Write metrics to Redis for dashboard visibility
            if self._redis:
                await self._redis.hset("scaler:metrics", mapping={
                    "pending": pending,
                    "dynamic_containers": dynamic,
                    "last_check": int(time.time()),
                })

            if pending >= SCALE_UP_THRESHOLD and dynamic < total_dynamic_possible:
                # Scale up
                self._dynamic_count += 1
                name = spawn_kitchen_agent(self._docker, self._dynamic_count)
                logger.info(
                    "SCALE UP  reason=pending(%d)>=%d  spawned=%s",
                    pending, SCALE_UP_THRESHOLD, name,
                )
                if self._redis:
                    await self._redis.rpush(
                        "scaler:events",
                        f"scale_up|pending={pending}|container={name}|ts={int(time.time())}",
                    )

            elif pending < SCALE_DOWN_THRESHOLD and dynamic > 0:
                # Scale down
                name = remove_kitchen_agent(self._docker)
                if name:
                    logger.info(
                        "SCALE DOWN  reason=pending(%d)<%d  removed=%s",
                        pending, SCALE_DOWN_THRESHOLD, name,
                    )
                    if self._redis:
                        await self._redis.rpush(
                            "scaler:events",
                            f"scale_down|pending={pending}|container={name}|ts={int(time.time())}",
                        )

        except docker.errors.DockerException as exc:
            logger.error("Docker error during scaling: %s", exc)
        except Exception as exc:
            logger.error("Unexpected scaler error: %s", exc)

    async def run(self) -> None:
        await self.start()
        try:
            while True:
                await self.tick()
                await asyncio.sleep(POLL_INTERVAL)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
async def main() -> None:
    scaler = AutoScaler()
    task = asyncio.create_task(scaler.run())
    try:
        await task
    except (KeyboardInterrupt, asyncio.CancelledError):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
