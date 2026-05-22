"""Tests de validation des schemas Pydantic XFlow V2."""
from __future__ import annotations

import pytest
from pydantic import ValidationError
from app.xflow.src.schemas.workflow import (
    ActionStep, ConditionStep, ForeachStep, ParallelStep,
    SwitchStep, TriggerConfig, TriggerType, WorkflowDefinition,
)


class TestWorkflowDefinition:
    def _minimal(self, extra_steps=None):
        steps = [{"id": "s1", "type": "action", "plugin": "mail", "action": "send"}]
        if extra_steps:
            steps.extend(extra_steps)
        return {"name": "test_wf", "steps": steps}

    def test_minimal_valid(self):
        d = WorkflowDefinition(**self._minimal())
        assert d.name == "test_wf"
        assert len(d.steps) == 1
        assert d.start_step_id == "s1"

    def test_entry_step_override(self):
        data = self._minimal([{"id": "s2", "type": "action", "plugin": "x", "action": "y"}])
        data["entry_step"] = "s2"
        d = WorkflowDefinition(**data)
        assert d.start_step_id == "s2"

    def test_empty_steps_raises(self):
        with pytest.raises(ValidationError):
            WorkflowDefinition(name="x", steps=[])

    def test_auto_dispatch_step_types(self):
        data = {
            "name": "multi",
            "steps": [
                {"id": "a", "type": "action", "plugin": "p", "action": "x"},
                {"id": "b", "type": "condition", "condition": {"left": "{{ x }}", "operator": "==", "right": "1"}},
                {"id": "c", "type": "wait", "delay_seconds": 5},
                {"id": "d", "type": "switch", "expression": "{{ y }}", "cases": {"v": "a"}},
            ]
        }
        d = WorkflowDefinition(**data)
        assert isinstance(d.steps[0], ActionStep)
        assert isinstance(d.steps[1], ConditionStep)

    def test_export_graph(self):
        d = WorkflowDefinition(**{
            "name": "graph_test",
            "steps": [
                {"id": "s1", "type": "action", "plugin": "p", "action": "x", "on_success": "s2"},
                {"id": "s2", "type": "action", "plugin": "p", "action": "y"},
            ]
        })
        g = d.export_graph()
        assert len(g["nodes"]) == 2
        assert any(e["label"] == "success" for e in g["edges"])

    def test_trigger_event(self):
        d = WorkflowDefinition(**{
            **self._minimal(),
            "trigger": {"type": "event", "event_name": "user.created"}
        })
        assert d.trigger.type == TriggerType.EVENT
        assert d.trigger.event_name == "user.created"
