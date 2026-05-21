"""
Tests for LLM agent — analyze_order, rule-based fallback, bid handler.
"""
from __future__ import annotations

import json
import sys
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "agents", "llm_agent"))

import main as llm_main


class TestRuleBasedFallback:
    def test_steak_recommendation(self) -> None:
        result = llm_main._rule_based_fallback("клиент заказал стейк и пиво")
        assert "вино" in result.lower() or "соус" in result.lower()

    def test_soup_recommendation(self) -> None:
        result = llm_main._rule_based_fallback("заказали борщ")
        assert "хлеб" in result.lower() or "пампушк" in result.lower()

    def test_tea_recommendation(self) -> None:
        result = llm_main._rule_based_fallback("заказали чай и воду")
        assert "десерт" in result.lower()

    def test_unknown_order_returns_default(self) -> None:
        result = llm_main._rule_based_fallback("что-то непонятное")
        assert len(result) > 0

    def test_returns_string(self) -> None:
        result = llm_main._rule_based_fallback("любой текст")
        assert isinstance(result, str)


class TestCallLlm:
    @pytest.mark.asyncio
    async def test_no_api_key_uses_fallback(self) -> None:
        original = llm_main.ANTHROPIC_API_KEY
        llm_main.ANTHROPIC_API_KEY = ""
        try:
            result = await llm_main.call_llm("тест промпт про стейк")
            assert isinstance(result, str)
            assert len(result) > 0
        finally:
            llm_main.ANTHROPIC_API_KEY = original

    @pytest.mark.asyncio
    async def test_api_error_falls_back_to_rule_based(self) -> None:
        llm_main.ANTHROPIC_API_KEY = "test-key"
        mock_client = AsyncMock()
        mock_client.messages.create = AsyncMock(side_effect=Exception("API error"))

        with patch("anthropic.AsyncAnthropic", return_value=mock_client):
            result = await llm_main.call_llm("клиент заказал борщ")

        assert isinstance(result, str)
        assert len(result) > 0
        llm_main.ANTHROPIC_API_KEY = ""

    @pytest.mark.asyncio
    async def test_successful_api_call_returns_response(self) -> None:
        llm_main.ANTHROPIC_API_KEY = "test-key"
        mock_message = MagicMock()
        mock_message.content = [MagicMock(text="Отличный выбор! Рекомендуем десерт.")]
        mock_client = AsyncMock()
        mock_client.messages.create = AsyncMock(return_value=mock_message)

        with patch("anthropic.AsyncAnthropic", return_value=mock_client):
            result = await llm_main.call_llm("клиент заказал стейк")

        assert result == "Отличный выбор! Рекомендуем десерт."
        llm_main.ANTHROPIC_API_KEY = ""


class TestAnalyzeOrder:
    @pytest.mark.asyncio
    async def test_analyze_order_returns_required_fields(self) -> None:
        with patch.object(llm_main, "call_llm", new=AsyncMock(return_value="Рекомендация")):
            result = await llm_main.analyze_order({
                "order_id": "test-123",
                "items": [{"name": "Борщ", "price": 350.0, "quantity": 1}],
                "total": 350.0,
                "customer_name": "Иван",
            })

        assert result["order_id"] == "test-123"
        assert result["recommendation"] == "Рекомендация"
        assert result["status"] == "analyzed"
        assert result["agent_id"] == llm_main.AGENT_ID
        assert "llm_elapsed_s" in result

    @pytest.mark.asyncio
    async def test_analyze_order_handles_empty_items(self) -> None:
        with patch.object(llm_main, "call_llm", new=AsyncMock(return_value="ok")):
            result = await llm_main.analyze_order({"order_id": "x", "items": [], "total": 0})

        assert result["items_analyzed"] == []
        assert result["status"] == "analyzed"

    @pytest.mark.asyncio
    async def test_analyze_order_formats_items_in_prompt(self) -> None:
        captured_prompt = []

        async def capture_llm(prompt: str) -> str:
            captured_prompt.append(prompt)
            return "ok"

        with patch.object(llm_main, "call_llm", new=capture_llm):
            await llm_main.analyze_order({
                "order_id": "y",
                "items": [{"name": "Пицца", "price": 500.0, "quantity": 2}],
                "total": 1000.0,
                "customer_name": "Мария",
            })

        assert "Пицца x2" in captured_prompt[0]
        assert "Мария" in captured_prompt[0]
        assert "1000" in captured_prompt[0]
