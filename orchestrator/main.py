"""Orchestrator service entry point."""
import asyncio
import json
import logging
import os

import nats
import redis.asyncio as aioredis

from orchestrator import RestaurantOrchestrator

logger = logging.getLogger("orchestrator.main")
NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


async def main() -> None:
    orch = RestaurantOrchestrator()
    await orch.start()

    # Subscribe to incoming orders via NATS subject orders.new
    nc = orch._nc  # noqa: SLF001 (internal access for demo)
    assert nc is not None

    async def on_order(msg: nats.aio.client.Msg) -> None:
        try:
            order = json.loads(msg.data.decode())
            result = await orch.process_order(order)
            logger.info("Order complete: %s", result.get("order_id"))
        except Exception as exc:
            logger.error("Order processing error: %s", exc)

    await nc.subscribe("orders.new", cb=on_order)
    logger.info("Listening for orders on 'orders.new'")

    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await orch.stop()


if __name__ == "__main__":
    asyncio.run(main())
