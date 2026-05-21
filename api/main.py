"""
Restaurant MAS — FastAPI REST API.

Endpoints:
  POST /orders          → start full pipeline
  GET  /orders/{id}     → get order status from Redis
  GET  /metrics         → orchestrator + agent metrics
  GET  /health          → liveness probe
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Any

import nats
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator

import sys
sys.path.insert(0, "/app/orchestrator")

from orchestrator import RestaurantOrchestrator

logger = logging.getLogger("api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

orch: RestaurantOrchestrator | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    global orch
    orch = RestaurantOrchestrator()
    await orch.start()
    yield
    if orch:
        await orch.stop()


app = FastAPI(
    title="Restaurant MAS API",
    description="Multi-agent system for restaurant order processing — LAB13, Variant 10",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------
class OrderItem(BaseModel):
    name: str = Field(..., min_length=1)
    price: float = Field(..., gt=0)
    quantity: int = Field(default=1, ge=1)


class OrderRequest(BaseModel):
    table_number: int = Field(..., ge=1, le=50)
    items: list[OrderItem] = Field(..., min_length=1)
    customer_name: str = Field(default="Guest", min_length=1)

    @field_validator("items")
    @classmethod
    def items_not_empty(cls, v: list[OrderItem]) -> list[OrderItem]:
        if not v:
            raise ValueError("Order must have at least one item")
        return v


class OrderResponse(BaseModel):
    order_id: str
    status: str
    table_number: int | None = None
    total: float | None = None
    delivered_at: str | None = None
    steps: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(request: OrderRequest) -> OrderResponse:
    """
    Submit a new restaurant order and run the full MAS pipeline.

    Pipeline: validate_order → assign_table → cook_dish → deliver_order
    """
    if orch is None:
        raise HTTPException(status_code=503, detail="Orchestrator not ready")

    order_id = str(uuid.uuid4())
    order_payload = {
        "order_id": order_id,
        "table_number": request.table_number,
        "customer_name": request.customer_name,
        "items": [item.model_dump() for item in request.items],
    }

    try:
        result = await orch.process_order(order_payload)
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return OrderResponse(**result)


@app.get("/orders/{order_id}", response_model=dict[str, Any])
async def get_order(order_id: str) -> dict[str, Any]:
    """Retrieve current order status from Redis."""
    if orch is None:
        raise HTTPException(status_code=503, detail="Orchestrator not ready")

    status = await orch.get_order_status(order_id)
    if not status:
        raise HTTPException(status_code=404, detail="Order not found")
    return status


@app.get("/metrics", response_model=dict[str, Any])
async def get_metrics() -> dict[str, Any]:
    """Return orchestrator pipeline metrics."""
    if orch is None:
        return {"error": "not ready"}
    metrics = await orch.get_metrics()

    # Also fetch per-agent counters from Redis
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        keys = await redis_client.keys("kitchen:agent:*:processed")
        agent_stats = {}
        for key in keys:
            agent_id = key.split(":")[2]
            count = await redis_client.get(key)
            agent_stats[agent_id] = int(count or 0)
        metrics["agent_stats"] = agent_stats
    except Exception as exc:
        logger.warning("metrics redis error: %s", exc)
    finally:
        await redis_client.aclose()

    return metrics


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok", "service": "restaurant-mas-api"}
