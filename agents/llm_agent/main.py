"""
LLM Agent — интеллектуальный агент на Python.

Задача: получить заказ из очереди, проанализировать его через LLM (Anthropic API)
и вернуть персональные рекомендации к заказу (что ещё заказать, предупреждения
о времени ожидания, пожелания к готовке).

Тип задачи: analyze_order
NATS: подписывается на tasks.bid (аукцион) и tasks.direct.llm-agent-1 (прямой вызов)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import time

import nats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("llm-agent")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
AGENT_ID = os.getenv("AGENT_ID", "llm-agent-1")
# Anthropic API key — передаётся через env
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")

processed = 0
active = 0


# ---------------------------------------------------------------------------
# LLM call
# ---------------------------------------------------------------------------
async def call_llm(prompt: str) -> str:
    """Call Anthropic API asynchronously. Falls back to rule-based if no key."""
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — using rule-based fallback")
        return _rule_based_fallback(prompt)

    try:
        import anthropic  # imported lazily so agent starts without the package

        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_API_KEY)
        message = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as exc:
        logger.error("LLM call failed: %s — using fallback", exc)
        return _rule_based_fallback(prompt)


def _rule_based_fallback(prompt: str) -> str:
    """Simple rule-based analysis when LLM is unavailable."""
    recommendations = []
    if "стейк" in prompt.lower() or "мясо" in prompt.lower():
        recommendations.append("К мясным блюдам рекомендуем красное вино или соус барбекю.")
    if "суп" in prompt.lower() or "борщ" in prompt.lower():
        recommendations.append("К супу рекомендуем свежий хлеб или пампушки.")
    if "чай" in prompt.lower() or "кофе" in prompt.lower():
        recommendations.append("К напиткам рекомендуем десерт — тирамису или чизкейк.")
    if not recommendations:
        recommendations.append("Приятного аппетита! Если нужно что-то ещё — обращайтесь.")
    return " ".join(recommendations)


# ---------------------------------------------------------------------------
# Order analysis
# ---------------------------------------------------------------------------
async def analyze_order(payload: dict) -> dict:
    """
    Main intelligence task: analyze the order and return LLM-generated
    recommendations.
    """
    order_id = payload.get("order_id", "unknown")
    items = payload.get("items", [])
    customer_name = payload.get("customer_name", "Гость")
    total = payload.get("total", 0)

    item_names = [
        f"{item.get('name', '?')} x{item.get('quantity', 1)}"
        for item in items
    ]
    items_str = ", ".join(item_names) if item_names else "нет позиций"

    prompt = (
        f"Ты — помощник ресторана. Клиент {customer_name} сделал заказ: {items_str}. "
        f"Сумма заказа: {total} руб. "
        f"Дай краткие (2-3 предложения) персональные рекомендации: "
        f"что ещё можно заказать, предупреждения о времени ожидания для сложных блюд, "
        f"или пожелания к подаче. Отвечай по-русски."
    )

    logger.info("Calling LLM for order_id=%s items=%s", order_id, items_str)
    start = time.time()
    recommendation = await call_llm(prompt)
    elapsed = round(time.time() - start, 2)

    logger.info("LLM response received  order_id=%s  elapsed=%.2fs", order_id, elapsed)

    return {
        "order_id": order_id,
        "customer_name": customer_name,
        "recommendation": recommendation,
        "items_analyzed": item_names,
        "llm_elapsed_s": elapsed,
        "agent_id": AGENT_ID,
        "status": "analyzed",
    }


# ---------------------------------------------------------------------------
# Cost for auction
# ---------------------------------------------------------------------------
def current_cost() -> float:
    return 1.0 + active * 2.0


# ---------------------------------------------------------------------------
# NATS handlers
# ---------------------------------------------------------------------------
def make_task_handler(nc):
    async def handle_task(msg):
        global processed, active
        processed += 1
        active += 1
        start = time.time()

        try:
            data = json.loads(msg.data.decode())
        except Exception as exc:
            logger.error("unmarshal task: %s", exc)
            active -= 1
            return

        if data.get("type") != "analyze_order":
            active -= 1
            return

        task_id = data["id"]
        logger.info("processing task  task_id=%s  agent=%s", task_id, AGENT_ID)

        try:
            output = await analyze_order(data.get("payload", {}))
            result = {
                "task_id": task_id,
                "success": True,
                "output": output,
                "error": "",
                "agent_id": AGENT_ID,
                "trace_id": data.get("trace_id", ""),
            }
        except Exception as exc:
            logger.error("analyze_order failed: %s", exc)
            result = {
                "task_id": task_id,
                "success": False,
                "output": {},
                "error": str(exc),
                "agent_id": AGENT_ID,
                "trace_id": data.get("trace_id", ""),
            }

        await nc.publish("tasks.completed", json.dumps(result).encode())
        active -= 1
        logger.info(
            "task done  task_id=%s  success=%s  elapsed_ms=%d  processed_total=%d",
            task_id, result["success"], int((time.time() - start) * 1000), processed,
        )

    return handle_task


def make_bid_handler(nc):
    async def handle_bid(msg):
        try:
            data = json.loads(msg.data.decode())
        except Exception:
            return
        if data.get("type") != "analyze_order":
            return
        bid = {
            "task_id": data["id"],
            "agent_id": AGENT_ID,
            "cost": current_cost(),
            "task_type": data["type"],
        }
        await nc.publish("tasks.bids", json.dumps(bid).encode())
        logger.debug("bid submitted  task_id=%s  cost=%.1f", data["id"], bid["cost"])

    return handle_bid


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    nc = await nats.connect(
        NATS_URL,
        reconnect_time_wait=2,
        max_reconnect_attempts=-1,
    )

    task_handler = make_task_handler(nc)
    bid_handler = make_bid_handler(nc)

    # Auction: respond to bid requests
    await nc.subscribe("tasks.bid", cb=bid_handler)

    # Direct: receive tasks won at auction
    await nc.subscribe(f"tasks.direct.{AGENT_ID}", cb=task_handler)

    # Fallback: broadcast (also handles queue-based dispatch)
    await nc.subscribe("tasks.process", cb=task_handler)

    logger.info("llm-agent started  agent_id=%s  nats=%s", AGENT_ID, NATS_URL)
    if not ANTHROPIC_API_KEY:
        logger.warning("ANTHROPIC_API_KEY not set — rule-based fallback active")

    await stop_event.wait()
    await nc.drain()
    logger.info("llm-agent stopped  processed=%d", processed)


if __name__ == "__main__":
    asyncio.run(main())
