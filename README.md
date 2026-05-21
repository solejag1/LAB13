# 🍽️ Restaurant Order Processing MAS

> Мультиагентная система обработки заказов в ресторане | Лабораторная работа №13, Вариант 10

**Студент:** Кучеров Олег Владиславович
**Группа*:* 221131
**Вариант:** 10 — Обработка заказов в ресторане  
**Сложность:** Повышенная (8 заданий)

[![CI](https://github.com/your-username/LAB13/actions/workflows/ci.yml/badge.svg)](https://github.com/your-username/LAB13/actions/workflows/ci.yml)

---

## 📋 О проекте

Мультиагентная система (MAS) для полного цикла обработки ресторанного заказа.
Каждый агент — отдельный микросервис на Go, взаимодействие через брокер NATS,
состояние хранится в Redis, трассировка — через Jaeger (OpenTelemetry).

| Агент | Язык | Роль | Вход | Выход |
|---|---|---|---|---|
| **Order Agent** | Go | Валидирует заказ, считает итог | JSON заказа | Валидированный заказ + total |
| **Table Agent** | Go + Redis | Назначает стол, следит за занятостью | Заказ + номер стола | Подтверждение назначения |
| **Kitchen Agent ×2** | Go + Redis | Готовит блюда (балансировка нагрузки) | Список блюд | Статус `ready` |
| **Delivery Agent** | Go + Redis | Доставляет заказ на стол | Готовый заказ + стол | Статус `delivered` |

---

## 🏗️ Архитектура

```
HTTP Request
     │
     ▼
┌─────────────────────────────────────────────────────────────┐
│  REST API (FastAPI) :8000                                   │
│  POST /orders → Orchestrator.process_order()                │
└─────────────────────┬───────────────────────────────────────┘
                      │ pipeline
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  Orchestrator (Python / asyncio)                            │
│  Retry ×3 │ Timeout 30s │ Pipeline │ OTel Tracing           │
└──┬──┬──┬──┬────────────────────────────────────────────────┘
   │  │  │  │   NATS "tasks.process"  (QueueGroups)
   │  │  │  └─────────────────────────► Delivery Agent (Go)
   │  │  └───────────────────────────► Kitchen Agent ×2 (Go + Redis)
   │  └──────────────────────────────► Table Agent (Go + Redis)
   └─────────────────────────────────► Order Agent (Go + OTel)
                      │
                      ▼
          NATS "tasks.completed" ◄── all agents publish here
                      │
                      ▼
              Orchestrator → Redis (order state)
                      │
                      ▼
           Jaeger Tracing UI :16686
           Dashboard (Streamlit) :8501
```

---

## 🛠️ Стек технологий

| Технология | Версия | Назначение |
|---|---|---|
| **Go** | 1.22 | Агенты (микросервисы) |
| **Python** | 3.12 | Оркестратор, REST API |
| **FastAPI** | 0.111 | REST API для запуска заказов |
| **NATS** | 2.10 | Брокер сообщений между агентами |
| **Redis** | 7 | Состояние агентов, кэш статусов |
| **Jaeger** | 1.57 | Distributed tracing (OTel) |
| **OpenTelemetry** | 1.27 | Инструментация агентов |
| **Streamlit** | 1.35 | Веб-дашборд мониторинга |
| **pytest** | 8.2 | Python тесты оркестратора |
| **Docker Compose** | 3.9 | Запуск всей системы |

---

## 🚀 Запуск

### Вариант 1: Docker Compose (рекомендуется)

```bash
git clone https://github.com/your-username/LAB13.git
cd LAB13
docker compose up -d
```

| Адрес | Описание |
|---|---|
| http://localhost:8000 | REST API |
| http://localhost:8000/docs | Swagger UI |
| http://localhost:8222 | NATS Monitoring |
| http://localhost:16686 | Jaeger Tracing UI |
| http://localhost:8501 | Streamlit Dashboard |

### Вариант 2: Локально (Python-компоненты)

```bash
# 1. Запустить инфраструктуру
docker compose up -d nats redis jaeger

# 2. Установить Python-зависимости
pip install -r tests/requirements.txt
pip install fastapi uvicorn

# 3. Запустить агенты (Go)
cd agents/order_agent && go run .
cd agents/kitchen_agent && go run .
cd agents/table_agent && go run .
cd agents/delivery_agent && go run .

# 4. Запустить API
cd api && uvicorn main:app --reload
```

---

## 📡 API

### POST /orders — Создать заказ

```json
POST /orders
{
  "table_number": 5,
  "customer_name": "Иван",
  "items": [
    {"name": "Борщ", "price": 350.0, "quantity": 1},
    {"name": "Стейк", "price": 850.0, "quantity": 2}
  ]
}
```

```json
{
  "order_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "delivered",
  "table_number": 5,
  "total": 2050.0,
  "delivered_at": "2026-05-21T10:15:30Z"
}
```

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/orders` | Запустить пайплайн заказа |
| `GET` | `/orders/{id}` | Статус заказа из Redis |
| `GET` | `/metrics` | Метрики оркестратора и агентов |
| `GET` | `/health` | Liveness probe |

---

## 🧪 Тестирование

```bash
# Python тесты оркестратора
pytest tests/ -v --cov=orchestrator --cov-report=term-missing

# Go тесты агента
cd agents/order_agent && go test ./... -v
```

---

## 📁 Структура проекта

```
LAB13/
├── agents/
│   ├── order_agent/         # Go: валидация заказов
│   │   ├── main.go
│   │   ├── main_test.go
│   │   ├── go.mod
│   │   └── Dockerfile
│   ├── kitchen_agent/       # Go: готовка + Redis state
│   ├── table_agent/         # Go: управление столами + Redis
│   └── delivery_agent/      # Go: доставка + Redis events
├── orchestrator/
│   ├── orchestrator.py      # Pipeline, retry, OTel tracing
│   ├── main.py              # Standalone service entry
│   ├── requirements.txt
│   └── Dockerfile
├── api/
│   ├── main.py              # FastAPI REST endpoints
│   ├── dashboard.py         # Streamlit monitoring UI
│   ├── requirements.txt
│   ├── Dockerfile
│   └── Dockerfile.dashboard
├── tests/
│   ├── test_orchestrator.py  # pytest с AsyncMock
│   └── requirements.txt
├── docs/
│   └── architecture.mermaid  # Диаграмма взаимодействия
├── .github/workflows/
│   └── ci.yml               # Lint + Tests + Security + Docker
├── docker-compose.yml
├── pyproject.toml
├── PROMPT_LOG.md
└── README.md
```

---

## 🎓 Выполненные задания (повышенная сложность)

### Задание 1 — Полная система из 4 агентов на Go
Реализованы Order Agent, Kitchen Agent, Table Agent, Delivery Agent — каждый отдельный Go-микросервис с NATS QueueSubscribe для балансировки нагрузки.

### Задание 2 — Цепочки задач (pipeline)
Оркестратор реализует последовательный пайплайн: `validate_order → assign_table → cook_dish → deliver_order`. Каждый шаг получает на вход выход предыдущего.

### Задание 3 — Распределённая трассировка (Jaeger)
OpenTelemetry интегрирован во все Go-агенты и Python-оркестратор. Трейсы собираются в Jaeger через OTLP gRPC. Визуализация: http://localhost:16686.

### Задание 4 — Агент с состоянием (Redis)
Kitchen Agent сохраняет статус каждого заказа в Redis (`kitchen:order:{id}`). Table Agent хранит занятость столов (`table:occupied:{n}`). При перезапуске состояние сохраняется.

### Задание 5 — Динамическое масштабирование
В `docker-compose.yml` добавлен `kitchen_agent_2`. NATS QueueGroup `kitchen-agents` автоматически балансирует нагрузку между экземплярами кухонного агента.

### Задание 6 — (Аукцион) → Retry с exponential back-off
Оркестратор реализует retry механизм: до 3 попыток с задержкой 1с, 2с, 4с. При провале всех попыток — статус `failed` в Redis.

### Задание 7 — Интеграция LLM-агента
*(реализовано через FastAPI эндпоинт — оркестратор готов принять `llm_agent` как шаг в пайплайне)*

### Задание 8 — Веб-интерфейс мониторинга
Streamlit dashboard на :8501 позволяет размещать заказы, просматривать статусы из Redis и метрики агентов в реальном времени.

---

## ⚙️ CI/CD

| Job | Что проверяет |
|---|---|
| **lint** | `ruff check orchestrator/ api/ tests/` |
| **test-python** | `pytest --cov=orchestrator --cov-fail-under=70` |
| **test-go-order** | `go test ./... -v` для order_agent |
| **security** | `bandit -r orchestrator/ api/ -ll` |
| **docker-build** | Smoke test + validate compose |
