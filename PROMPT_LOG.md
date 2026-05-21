# Prompt Log — Лабораторная работа №13

**Студент:** Кучеров Олег  
**Вариант:** 10 — Обработка заказов в ресторане  
**Сложность:** Повышенная  
Журнал всех промптов и ответов ИИ в процессе выполнения работы.

---

## Промпт 0.1 — Инициализация структуры проекта

**Дата:** 2026-05-21

**Промпт:**

```
Инициализируй git-репозиторий для LAB13 «Мультиагентная система ресторана».
Создай структуру: agents/{order_agent,kitchen_agent,table_agent,delivery_agent},
orchestrator/, api/, tests/, docs/, .github/workflows/.
Вариант 10 (средняя сложность) → выполняем повышенную сложность (8 заданий).
Создай docker-compose.yml с сервисами: nats, redis, jaeger, 4 агента, оркестратор,
api, dashboard. Коммит: «chore: initialize LAB13 project structure».
```

**Результат:** Создана полная файловая структура. `docker-compose.yml` содержит 10 сервисов: nats (2.10-alpine, порт 4222/8222), redis (7-alpine), jaeger (all-in-one 1.57, OTLP enabled), 4 Go-агента, оркестратор, FastAPI, Streamlit dashboard. Healthcheck + `depends_on: service_healthy` для NATS и Redis.

---

## Промпт 0.2 — Общие типы данных

**Дата:** 2026-05-21

**Промпт:**

```
Создай shared_types.py с dataclasses Task, TaskResult и enum-типами TaskType,
OrderStatus. Task: id, type, payload, trace_id, retry_count.
TaskResult: task_id, success, output, error, agent_id, trace_id.
```

**Результат:** Создан `shared_types.py` с 4 типами. `TaskType`: validate_order, cook_dish, assign_table, deliver_order. `OrderStatus`: received → validated → cooking → ready → delivering → delivered → failed.

---

## Промпт 1.1 — Order Agent на Go

**Дата:** 2026-05-21

**Промпт:**

```
Ты — senior Go разработчик. Реализуй Order Agent (agents/order_agent/main.go).
Агент подписывается на NATS subject "tasks.process" в QueueGroup "order-agents".
Обрабатывает задачи типа validate_order: проверяет наличие order_id, items (>0),
table_number (>0), считает total = sum(price * quantity).
Публикует результат в "tasks.completed".
OpenTelemetry: инициализировать TracerProvider с OTLP gRPC, создавать span на каждую задачу.
Логирование через log/slog с JSON handler.
Счётчик processed++. Non-root user в Dockerfile multi-stage.
Коммит: «feat(agents): add Order Agent with NATS and OTel».
```

**Результат:** Реализован `agents/order_agent/main.go` (170 строк). `validateOrder()` — чистая функция без side-effects (удобна для тестирования). QueueSubscribe с группой `order-agents` для балансировки. `initTracer()` с OTLP gRPC + InsecureSkipVerify. При ошибке трейсера — graceful degradation (продолжает работу). Dockerfile 2-stage (builder/alpine), `adduser appuser`. Написаны 8 unit-тестов в `main_test.go`.

---

## Промпт 1.2 — Kitchen Agent на Go с Redis

**Дата:** 2026-05-21

**Промпт:**

```
Реализуй Kitchen Agent (agents/kitchen_agent/main.go).
Подписывается на tasks.process в QueueGroup "kitchen-agents" — балансировка между
несколькими экземплярами. Тип задачи: cook_dish.
Redis: при старте приготовления писать HSET kitchen:order:{id} status=cooking,
при завершении status=ready. Инкрементировать счётчик kitchen:agent:{id}:processed.
Симулировать время готовки: len(items)*200ms, max 2s.
go.mod должен включать github.com/redis/go-redis/v9.
Коммит: «feat(agents): add Kitchen Agent with Redis state».
```

**Результат:** Реализован `agents/kitchen_agent/main.go` (230 строк). `cookDish()` использует Redis Pipeline для атомарной записи start + счётчик за 1 roundtrip. `QueueSubscribe("tasks.process", "kitchen-agents")` — оба экземпляра кухни (`kitchen_agent` и `kitchen_agent_2`) получают разные задачи. При недоступности Redis — graceful degradation (stateless mode). Кулинарное время пропорционально количеству блюд.

---

## Промпт 1.3 — Table Agent на Go с Redis

**Дата:** 2026-05-21

**Промпт:**

```
Реализуй Table Agent. Тип задачи: assign_table.
Если table_number не указан — найти свободный стол (1..20) через Redis EXISTS.
Иначе — занять указанный стол: SET table:occupied:{n} order_id с TTL 4h.
Хранить HSET table:order:{n} с метаданными. Вернуть assigned table_number.
Коммит: «feat(agents): add Table Agent with Redis occupancy».
```

**Результат:** Реализован `agents/table_agent/main.go` (190 строк). `assignTable()` реализует linear scan по 20 столам для поиска свободного. Redis Pipeline записывает `table:occupied:{n}` + `table:order:{n}` атомарно. TTL 4 часа — достаточно для ресторанного вечера. При занятых всех столах — возвращает ошибку `"no free tables available"`.

---

## Промпт 1.4 — Delivery Agent на Go с Redis Events

**Дата:** 2026-05-21

**Промпт:**

```
Реализуй Delivery Agent. Тип задачи: deliver_order.
При доставке: обновить HSET order:status:{id} status=delivered, delivered_at, delivered_by.
Опубликовать в Redis PubSub "restaurant:events" событие order.delivered.
Симулировать ходьбу: 300ms. Вернуть delivered_at.
Коммит: «feat(agents): add Delivery Agent with Redis events».
```

**Результат:** Реализован `agents/delivery_agent/main.go` (185 строк). После доставки публикует событие в Redis PubSub `restaurant:events` — может быть использовано для real-time уведомлений через WebSocket. Все 4 Go-агента используют одинаковую структуру `Task`/`TaskResult` для совместимости с оркестратором.

---

## Промпт 2.1 — Оркестратор на Python

**Дата:** 2026-05-21

**Промпт:**

```
Ты — senior Python разработчик. Реализуй RestaurantOrchestrator в orchestrator/orchestrator.py.
Методы:
- start(): connect NATS + Redis, subscribe tasks.completed
- stop(): drain + aclose
- send_task(task, timeout): publish + await Future, retry ×3 c exponential backoff 1s/2s/4s
- process_order(order): pipeline в 4 шага, каждый шаг получает output предыдущего
- _save_status(order_id, status, data): HSET в Redis + append log
- get_order_status(order_id): HGETALL из Redis
- get_metrics(): processed/failed/pending

OpenTelemetry: root span "process_order", span.record_exception при ошибке.
Сохранять статус в Redis при каждом шаге и "failed" при ошибке.
Коммит: «feat(orchestrator): add pipeline orchestrator with retry and OTel».
```

**Результат:** Реализован `orchestrator/orchestrator.py` (200 строк). `send_task()` использует `asyncio.Future` для ожидания результата — не блокирует event loop. Exponential backoff: `await asyncio.sleep(2 ** (attempt-1))` → 1s, 2s, 4s. `process_order()` передаёт `{**r1.output, **r2.output}` в cook_dish — объединяет данные шагов. `_save_status()` дополнительно пишет в `order:log:{id}` (список событий для аудита). `init_tracer()` — graceful degradation при недоступности Jaeger.

---

## Промпт 2.2 — FastAPI REST API

**Дата:** 2026-05-21

**Промпт:**

```
Создай api/main.py на FastAPI.
POST /orders: принимает OrderRequest (table_number 1-50, items min 1, customer_name),
запускает process_order, возвращает OrderResponse (201).
GET /orders/{id}: статус из Redis (404 если нет).
GET /metrics: метрики оркестратора + per-agent Redis счётчики.
GET /health: {status: ok}.
Pydantic v2: field_validator на items_not_empty.
lifespan: orch.start/stop. CORSMiddleware allow_origins=["*"].
Коммит: «feat(api): add FastAPI REST endpoints».
```

**Результат:** Реализован `api/main.py` (130 строк). `OrderRequest` с Pydantic v2 `@field_validator`. `lifespan()` с context manager — корректный startup/shutdown. `/metrics` агрегирует Redis ключи `kitchen:agent:*:processed` для per-agent статистики. Все эндпоинты имеют `response_model`.

---

## Промпт 2.3 — Streamlit Dashboard

**Дата:** 2026-05-21

**Промпт:**

```
Создай api/dashboard.py на Streamlit.
Sidebar: форма нового заказа (стол, клиент, блюда с количеством) → POST /orders.
Колонка 1: метрики оркестратора (processed/failed/pending), per-agent stats.
Колонка 2: последние 10 заказов из Redis keys order:status:*.
Снизу: ASCII-архитектурная диаграмма системы.
Коммит: «feat(dashboard): add Streamlit monitoring dashboard».
```

**Результат:** Реализован `api/dashboard.py` (110 строк). Сайдбар с четырьмя блюдами и динамическим количеством через `number_input`. Spinner при обработке заказа. Цветные emoji для статусов заказа. `@st.cache_resource` для Redis connection. ASCII-диаграмма системы в `st.code`.

---

## Промпт 3.1 — Unit-тесты агентов Go

**Дата:** 2026-05-21

**Промпт:**

```
Напиши Go unit-тесты для order_agent в main_test.go.
Используй стандартный testing пакет — без внешних зависимостей.
Тесты для validateOrder():
- ValidPayload → total корректный
- MissingOrderID → error
- EmptyItems → error
- InvalidTableNumber → error
- TotalCalculation с несколькими позициями
- SingleItemDefaultQty → qty=1
- OrderIDPresentInOutput → passthrough
- TableNumberPresentInOutput → passthrough
Коммит: «test(agents): add Go unit tests for order_agent».
```

**Результат:** Создан `agents/order_agent/main_test.go` (120 строк) с 8 тестами. Все тесты работают без внешних зависимостей (нет NATS/Redis), тестируют только чистую бизнес-логику `validateOrder()`. `go test ./... -v` — все тесты зелёные.

---

## Промпт 3.2 — Unit-тесты оркестратора Python

**Дата:** 2026-05-21

**Промпт:**

```
Напиши pytest тесты для RestaurantOrchestrator в tests/test_orchestrator.py.
Используй AsyncMock для NATS и Redis — без реальной инфраструктуры.
TestSendTask:
- success → result returned
- timeout → retries + raises RuntimeError
- success increments processed counter
- retry on agent failure (3 attempts)
TestProcessOrder:
- full pipeline success → status=delivered
- generates order_id if missing
- saves failed status on error
TestGetOrderStatus:
- returns redis data
- not found → None
- get_metrics returns counters
Цель покрытие ≥ 70%.
Коммит: «test(orchestrator): add pytest async tests with AsyncMock».
```

**Результат:** Создан `tests/test_orchestrator.py` (~160 строк) с 10 тестами. `_inject_result()` — helper, симулирует агента через `asyncio.sleep(0.05)` + `future.set_result()`. `TestSendTask.test_retry_on_agent_failure` — счётчик `call_count` проверяет, что агент вызывается 3 раза при двух провалах. `TestProcessOrder.test_saves_failed_status` — проверяет вызов `_redis.hset` с `"failed"`.

---

## Промпт 4.1 — CI Pipeline

**Дата:** 2026-05-21

**Промпт:**

```
Создай .github/workflows/ci.yml с 5 jobs:
- lint: ruff check
- test-python: pytest --cov=orchestrator --cov-fail-under=70
- test-go-order: go test ./... -v в agents/order_agent
- security: bandit -r orchestrator/ api/ -ll
- docker-build: validate compose + smoke build
Коммит: «ci: add CI pipeline with lint, tests, security».
```

**Результат:** Создан `.github/workflows/ci.yml` с 5 независимыми jobs. Go тесты в `working-directory: ./agents/order_agent`. Python тесты с `env: NATS_URL=nats://localhost:4222` (тесты используют AsyncMock, NATS недоступен — ок). Bandit проверяет orchestrator и api. Docker validate compose для синтаксической проверки.

---

## Промпт 5.1 — Документация и диаграмма

**Дата:** 2026-05-21

**Промпт:**

```
Создай docs/architecture.mermaid с диаграммой sequence:
Client → API → Orchestrator → (NATS → 4 агента) → Redis → Jaeger.
Покажи все 4 шага пайплайна, QueueGroup балансировку, Redis-операции.
Создай README.md с разделами: О проекте (таблица агентов), Архитектура (ASCII),
Стек (таблица), Запуск Docker + локальный, API (примеры), Тестирование,
Структура, Задания (все 8), CI/CD.
Коммит: «docs: add Mermaid architecture diagram and comprehensive README».
```

**Результат:** Создан `docs/architecture.mermaid` (Mermaid sequenceDiagram) со всеми 4 шагами пайплайна, Redis-операциями и OTLP spans. `README.md` содержит ASCII-диаграмму архитектуры, таблицу 4 агентов с ролями/входами/выходами, описание всех 8 повышенных заданий, таблицы стека и CI jobs.
