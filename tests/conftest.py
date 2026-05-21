"""Pytest configuration for LAB13 tests."""
import os

# Disable OpenTelemetry SDK so tests don't try to connect to Jaeger
os.environ.setdefault("OTEL_SDK_DISABLED", "true")
os.environ.setdefault("NATS_URL", "nats://localhost:4222")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
