"""
Restaurant MAS Monitoring Dashboard.

Web UI for:
- Submitting test orders
- Viewing order status
- Monitoring agent metrics
- Viewing recent order logs from Redis
"""
import json
import os
import time

import redis
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

st.set_page_config(
    page_title="Restaurant MAS Dashboard",
    page_icon="🍽️",
    layout="wide",
)

st.title("🍽️ Restaurant MAS — Monitoring Dashboard")
st.caption("Лабораторная работа №13 | Вариант 10 | Обработка заказов в ресторане")


# ---------------------------------------------------------------------------
# Redis connection (cached)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_redis() -> redis.Redis:  # type: ignore[type-arg]
    return redis.from_url(REDIS_URL, decode_responses=True)


# ---------------------------------------------------------------------------
# Sidebar: Submit Order
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("📝 Новый заказ")
    table_num = st.number_input("Номер стола", min_value=1, max_value=50, value=3)
    customer = st.text_input("Имя клиента", value="Иван")

    st.subheader("Блюда")
    items = [
        {"name": "Борщ", "price": 350.0, "quantity": st.number_input("Борщ (порций)", 0, 10, 1)},
        {"name": "Стейк", "price": 850.0, "quantity": st.number_input("Стейк (порций)", 0, 10, 1)},
        {"name": "Салат Цезарь", "price": 280.0, "quantity": st.number_input("Цезарь (порций)", 0, 10, 0)},
        {"name": "Чай", "price": 120.0, "quantity": st.number_input("Чай (порций)", 0, 10, 2)},
    ]
    items = [i for i in items if i["quantity"] > 0]

    if st.button("🚀 Разместить заказ", use_container_width=True, type="primary"):
        if not items:
            st.error("Выберите хотя бы одно блюдо")
        else:
            with st.spinner("Обрабатывается..."):
                try:
                    resp = requests.post(
                        f"{API_URL}/orders",
                        json={"table_number": int(table_num), "customer_name": customer, "items": items},
                        timeout=60,
                    )
                    if resp.status_code == 201:
                        data = resp.json()
                        st.success(f"✅ Заказ #{data['order_id'][:8]}... выполнен!")
                        st.json(data)
                    else:
                        st.error(f"Ошибка {resp.status_code}: {resp.text}")
                except requests.exceptions.ConnectionError:
                    st.error("API недоступен. Убедитесь, что система запущена.")


# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
col1, col2 = st.columns(2)

# Metrics
with col1:
    st.subheader("📊 Метрики оркестратора")
    try:
        resp = requests.get(f"{API_URL}/metrics", timeout=5)
        if resp.ok:
            metrics = resp.json()
            m1, m2, m3 = st.columns(3)
            m1.metric("Обработано", metrics.get("processed", 0))
            m2.metric("Ошибок", metrics.get("failed", 0))
            m3.metric("В очереди", metrics.get("pending", 0))

            if "agent_stats" in metrics and metrics["agent_stats"]:
                st.subheader("🤖 Агенты кухни")
                for agent_id, count in metrics["agent_stats"].items():
                    st.metric(agent_id, f"{count} задач")
        else:
            st.warning("Метрики недоступны")
    except requests.exceptions.ConnectionError:
        st.info("API не запущен — метрики недоступны")

# Recent orders from Redis
with col2:
    st.subheader("📋 Последние заказы (Redis)")
    try:
        rdb = get_redis()
        keys = rdb.keys("order:status:*")
        if keys:
            for key in list(keys)[:10]:
                order_id = key.split(":")[-1]
                data = rdb.hgetall(key)
                status = data.get("status", "?")
                emoji = {
                    "received": "📥",
                    "validated": "✅",
                    "cooking": "👨‍🍳",
                    "delivered": "🍽️",
                    "failed": "❌",
                }.get(status, "❓")
                with st.expander(f"{emoji} {order_id[:12]}... — {status}"):
                    st.json(data)
        else:
            st.info("Заказов пока нет")
    except Exception as e:
        st.warning(f"Redis недоступен: {e}")

# Architecture diagram
st.divider()
st.subheader("🏗️ Архитектура системы")
st.code(
    """
HTTP Request
     │
     ▼
┌─────────────────────────────────────────────────────────┐
│  REST API (FastAPI)  :8000                              │
│  POST /orders → Orchestrator.process_order()            │
└─────────────┬───────────────────────────────────────────┘
              │  asyncio pipeline
              ▼
┌─────────────────────────────────────────────────────────┐
│  Orchestrator (Python / asyncio)                        │
│  Retry ×3 │ Timeout 30s │ OTel Tracing                 │
└──┬──┬──┬──┬─────────────────────────────────────────────┘
   │  │  │  │   NATS "tasks.process" (pub/sub)
   │  │  │  └──────────────► Delivery Agent (Go)
   │  │  └─────────────────► Kitchen Agent ×2 (Go + Redis)
   │  └────────────────────► Table Agent (Go + Redis)
   └───────────────────────► Order Agent (Go + OTel)
              │
              ▼
     NATS "tasks.completed"  ◄── all agents publish here
              │
              ▼
        Orchestrator collects results → Redis state
              │
              ▼
      Jaeger Tracing UI :16686
""",
    language="text",
)
