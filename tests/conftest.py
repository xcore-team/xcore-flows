"""Configuration pytest pour XFlow V2."""
import sys
import os

# Ajoute la racine du plugin dans le path pour les imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


@pytest.fixture
def sample_workflow_dict():
    return {
        "name": "sample_onboarding",
        "version": "1.0.0",
        "description": "Test workflow",
        "trigger": {"type": "manual"},
        "steps": [
            {
                "id": "create_workspace",
                "type": "action",
                "plugin": "users",
                "action": "create_workspace",
                "payload": {"client_id": "{{ trigger.client_id }}"},
                "on_success": "send_mail",
            },
            {
                "id": "send_mail",
                "type": "action",
                "plugin": "mail",
                "action": "send",
                "payload": {"to": "{{ trigger.email }}", "template": "welcome"},
            },
        ],
    }


@pytest.fixture
def base_context():
    return {
        "trigger": {"client_id": "C-001", "email": "test@example.com", "amount": "200"},
        "steps": {},
        "loop_item": None,
        "run": {"id": "run-123", "workflow": "sample_onboarding"},
        "context": {},
    }
