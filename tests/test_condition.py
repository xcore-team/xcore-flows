"""Tests unitaires pour l'évaluateur de conditions et templates."""
from __future__ import annotations

import pytest
from app.xflow.src.runtime.condition import evaluate_condition, render_payload, render_value
from app.xflow.src.schemas.workflow import ConditionConfig, ConditionOperator


CTX = {
    "trigger": {"email": "alice@example.com", "amount": "150", "plan": "pro"},
    "steps": {
        "get_invoices": {"result": {"count": 3, "invoices": [{"id": "INV-1"}, {"id": "INV-2"}]}}
    },
    "loop_item": {"id": "INV-42", "amount": 99.0},
}


class TestRenderValue:
    def test_simple_string_passthrough(self):
        assert render_value("hello", CTX) == "hello"

    def test_full_template_returns_native_type(self):
        assert render_value("{{ trigger.amount }}", CTX) == "150"

    def test_nested_path(self):
        assert render_value("{{ steps.get_invoices.result.count }}", CTX) == 3

    def test_inline_interpolation(self):
        result = render_value("Bonjour {{ trigger.email }} !", CTX)
        assert result == "Bonjour alice@example.com !"

    def test_dict_recursion(self):
        result = render_value({"to": "{{ trigger.email }}"}, CTX)
        assert result == {"to": "alice@example.com"}

    def test_list_recursion(self):
        result = render_value(["{{ trigger.plan }}", "fixed"], CTX)
        assert result == ["pro", "fixed"]


class TestEvaluateCondition:
    def _cond(self, left, op, right=None):
        return ConditionConfig(left=left, operator=op, right=right)

    def test_eq_true(self):
        assert evaluate_condition(self._cond("{{ trigger.plan }}", ConditionOperator.EQ, "pro"), CTX)

    def test_eq_false(self):
        assert not evaluate_condition(self._cond("{{ trigger.plan }}", ConditionOperator.EQ, "starter"), CTX)

    def test_numeric_gt(self):
        assert evaluate_condition(self._cond("{{ trigger.amount }}", ConditionOperator.GT, "100"), CTX)

    def test_numeric_lte(self):
        assert evaluate_condition(self._cond("{{ trigger.amount }}", ConditionOperator.LTE, "200"), CTX)

    def test_contains_string(self):
        assert evaluate_condition(self._cond("{{ trigger.email }}", ConditionOperator.CONTAINS, "example"), CTX)

    def test_starts_with(self):
        assert evaluate_condition(self._cond("{{ trigger.email }}", ConditionOperator.STARTS_WITH, "alice"), CTX)

    def test_is_null_missing(self):
        assert evaluate_condition(self._cond("{{ trigger.missing_key }}", ConditionOperator.IS_NULL), CTX)

    def test_is_not_null(self):
        assert evaluate_condition(self._cond("{{ trigger.plan }}", ConditionOperator.IS_NOT_NULL), CTX)

    def test_regex(self):
        assert evaluate_condition(
            self._cond("{{ trigger.email }}", ConditionOperator.REGEX, r"@\w+\.com"), CTX
        )
