"""
Catalogue statique des actions IPC et événements — chargé depuis .xcore/schemas.json.
Généré par : uv run xcli plugin security validate --save
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("xflow.discovery")

_DEFAULT_SCHEMA_PATH = Path(".xcore/schemas.json")


class DiscoveryService:
    """
    Charge le catalogue IPC depuis le fichier snapshot généré par xcli.

    Format du fichier :
        {
          "plugin_name": {
            "actions": { "action.name": { plugin, action, version, input, output, ... } },
            "events":  [ { plugin, event, method, priority, once }, ... ]
          }
        }
    """

    def __init__(self, schema_path: Path = _DEFAULT_SCHEMA_PATH) -> None:
        self._schema_path = schema_path
        self._actions: Dict[str, Dict[str, Any]] = {}   # "plugin:action" -> schema dict
        self._listeners_index: Dict[str, List[str]] = {}  # event -> [plugin, ...]
        self._raw: Dict[str, Dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load(self) -> None:
        """Charge le fichier schemas.json. Silencieux si absent."""
        if not self._schema_path.exists():
            logger.warning(
                "Catalogue IPC introuvable : %s — lancez 'xcli plugin security validate --save'.",
                self._schema_path,
            )
            return

        try:
            raw: Dict[str, Any] = json.loads(self._schema_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.error("Impossible de lire %s : %s", self._schema_path, exc)
            return

        self._raw = raw
        actions: Dict[str, Dict[str, Any]] = {}
        listeners: Dict[str, List[str]] = {}

        for plugin_name, data in raw.items():
            for action_name, schema in (data.get("actions") or {}).items():
                key = f"{plugin_name}:{action_name}"
                actions[key] = {**schema, "qualified_name": f"{plugin_name}.{action_name}"}

            for evt in (data.get("events") or []):
                event_name = evt.get("event", "")
                if event_name:
                    listeners.setdefault(event_name, []).append(plugin_name)

        self._actions = actions
        self._listeners_index = listeners

        action_count = len(actions)
        event_count = len(listeners)
        logger.info(
            "Catalogue chargé depuis %s — %d action(s), %d événement(s).",
            self._schema_path,
            action_count,
            event_count,
        )

    # ------------------------------------------------------------------
    # Actions IPC
    # ------------------------------------------------------------------

    def list_available_actions(self) -> List[Dict[str, Any]]:
        """Retourne la liste de toutes les actions IPC avec leurs schémas."""
        return [
            {
                "qualified_name": s["qualified_name"],
                "plugin": s.get("plugin", ""),
                "action": s.get("action", ""),
                "version": s.get("version", "0.0.0"),
                "description": s.get("description", ""),
                "input_schema": s.get("input", {}),
                "output_schema": s.get("output", {}),
            }
            for s in self._actions.values()
        ]

    def get_action_metadata(self, qualified_name: str) -> Optional[Dict[str, Any]]:
        """Retourne le schéma d'une action via 'plugin.action' ou 'plugin:action'."""
        key = qualified_name.replace(".", ":", 1) if ":" not in qualified_name else qualified_name
        return self._actions.get(key)

    def list_for_plugin(self, plugin_name: str) -> List[Dict[str, Any]]:
        return [s for s in self._actions.values() if s.get("plugin") == plugin_name]

    def plugin_count(self) -> int:
        return len(self._raw)

    # ------------------------------------------------------------------
    # Événements
    # ------------------------------------------------------------------

    def get_event_listeners(self, event_name: str) -> List[str]:
        return self._listeners_index.get(event_name, [])

    def list_all_events(self) -> Dict[str, List[str]]:
        """Retourne tous les événements connus et leurs abonnés."""
        return dict(self._listeners_index)

    def get_plugin_events(self, plugin_name: str) -> List[str]:
        """Retourne les événements auxquels un plugin est abonné."""
        return [
            evt
            for evt, plugins in self._listeners_index.items()
            if plugin_name in plugins
        ]
