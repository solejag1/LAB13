"""Shared data types for the restaurant MAS."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class TaskType(str, Enum):
    VALIDATE_ORDER = "validate_order"
    COOK_DISH = "cook_dish"
    ASSIGN_TABLE = "assign_table"
    DELIVER_ORDER = "deliver_order"


class OrderStatus(str, Enum):
    RECEIVED = "received"
    VALIDATED = "validated"
    COOKING = "cooking"
    READY = "ready"
    DELIVERING = "delivering"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass
class Task:
    id: str
    type: TaskType
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
