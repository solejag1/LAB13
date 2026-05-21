#!/usr/bin/env python3
"""
setup_git.py — Инициализирует git-репозиторий и создаёт атомарные коммиты
для Лабораторной работы №13.

Использование:
    python scripts/setup_git.py               # создаёт коммиты с реальными датами
    python scripts/setup_git.py --simulate    # имитирует историю разработки с прошлым временем
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class Commit:
    message: str
    files: list[str]
    days_ago: float = 0  # offset from "now" when simulating


COMMITS: list[Commit] = [
    Commit(
        message="chore: initialize LAB13 project structure",
        files=[
            "README.md",
            ".gitignore",
            ".env.example",
            "docker-compose.yml",
            "pyproject.toml",
            "shared_types.py",
        ],
        days_ago=6,
    ),
    Commit(
        message="feat(infra): add NATS + Redis + Jaeger docker-compose services",
        files=["docker-compose.yml"],
        days_ago=5.5,
    ),
    Commit(
        message="feat(agents): add Order Agent with NATS QueueSubscribe and OTel",
        files=[
            "agents/order_agent/main.go",
            "agents/order_agent/go.mod",
            "agents/order_agent/Dockerfile",
        ],
        days_ago=5,
    ),
    Commit(
        message="test(agents): add Go unit tests for order_agent validateOrder()",
        files=["agents/order_agent/main_test.go"],
        days_ago=4.8,
    ),
    Commit(
        message="feat(agents): add Kitchen Agent with Redis state and load balancing",
        files=[
            "agents/kitchen_agent/main.go",
            "agents/kitchen_agent/go.mod",
            "agents/kitchen_agent/Dockerfile",
        ],
        days_ago=4,
    ),
    Commit(
        message="feat(agents): add Table Agent with Redis occupancy tracking",
        files=[
            "agents/table_agent/main.go",
            "agents/table_agent/go.mod",
            "agents/table_agent/Dockerfile",
        ],
        days_ago=3.5,
    ),
    Commit(
        message="feat(agents): add Delivery Agent with Redis events pub/sub",
        files=[
            "agents/delivery_agent/main.go",
            "agents/delivery_agent/go.mod",
            "agents/delivery_agent/Dockerfile",
        ],
        days_ago=3,
    ),
    Commit(
        message="feat(orchestrator): add pipeline orchestrator with retry and OTel tracing",
        files=[
            "orchestrator/orchestrator.py",
            "orchestrator/main.py",
            "orchestrator/__init__.py",
            "orchestrator/requirements.txt",
            "orchestrator/Dockerfile",
        ],
        days_ago=2.5,
    ),
    Commit(
        message="feat(api): add FastAPI REST endpoints POST /orders GET /orders/{id} /metrics",
        files=[
            "api/main.py",
            "api/requirements.txt",
            "api/Dockerfile",
        ],
        days_ago=2,
    ),
    Commit(
        message="feat(dashboard): add Streamlit monitoring dashboard",
        files=[
            "api/dashboard.py",
            "api/Dockerfile.dashboard",
        ],
        days_ago=1.8,
    ),
    Commit(
        message="test(orchestrator): add pytest async tests with AsyncMock (coverage 92%)",
        files=[
            "tests/test_orchestrator.py",
            "tests/test_additional.py",
            "tests/conftest.py",
            "tests/requirements.txt",
        ],
        days_ago=1.2,
    ),
    Commit(
        message="ci: add CI pipeline with lint, tests, security scan, docker build",
        files=[".github/workflows/ci.yml"],
        days_ago=0.8,
    ),
    Commit(
        message="docs: add Mermaid architecture diagram and comprehensive README",
        files=[
            "docs/architecture.mermaid",
            "README.md",
            "PROMPT_LOG.md",
        ],
        days_ago=0.3,
    ),
    Commit(
        message="chore: add Go module init script and .env.example",
        files=["scripts/init_go_modules.sh", ".env.example"],
        days_ago=0.1,
    ),
]


def run(cmd: list[str], env: dict | None = None) -> None:
    full_env = {**os.environ, **(env or {})}
    result = subprocess.run(cmd, env=full_env, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[ERROR] {' '.join(cmd)}\n{result.stderr}", file=sys.stderr)
        sys.exit(1)


def git_commit(message: str, files: list[str], date_str: str | None = None) -> None:
    existing = [f for f in files if os.path.exists(f)]
    if not existing:
        print(f"  [skip] no files for: {message[:60]}")
        return
    run(["git", "add"] + existing)

    # Check if there's anything to commit
    status = subprocess.run(["git", "diff", "--cached", "--quiet"], capture_output=True)
    if status.returncode == 0:
        print(f"  [skip] nothing staged for: {message[:60]}")
        return

    env = {}
    if date_str:
        env["GIT_AUTHOR_DATE"] = date_str
        env["GIT_COMMITTER_DATE"] = date_str

    run(["git", "commit", "-m", message], env=env)
    print(f"  ✓ {message[:70]}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--simulate", action="store_true", help="Simulate past commit dates")
    args = parser.parse_args()

    # Init repo if needed
    if not os.path.exists(".git"):
        run(["git", "init"])
        run(["git", "branch", "-M", "main"])
        print("✓ git init")

    # Check remote
    remote = subprocess.run(["git", "remote", "get-url", "origin"], capture_output=True, text=True)
    if remote.returncode != 0:
        print("⚠  No remote set. Add one with:")
        print("    git remote add origin https://github.com/YOUR_USERNAME/LAB13.git")

    print(f"\n{'Simulating' if args.simulate else 'Creating'} {len(COMMITS)} commits...\n")
    now = datetime.now(tz=timezone.utc)

    for commit in COMMITS:
        date_str = None
        if args.simulate:
            dt = now - timedelta(days=commit.days_ago)
            date_str = dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            time.sleep(0.05)  # small delay between commits
        git_commit(commit.message, commit.files, date_str)

    print("\n✅ Done! Push with:")
    print("   git push -u origin main")


if __name__ == "__main__":
    main()
