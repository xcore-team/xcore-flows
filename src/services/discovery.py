"""
Service de découverte dynamique des actions IPC et des événements des plugins XCore — XFlow V2.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional, Protocol

from xcore.kernel.api import PluginContext

logger = logging.getLogger("xflow.discovery")


class _PluginLike(Protocol):
    ctx: PluginContext

    async def call_plugin(
        self, name: str, plugin_action: str, payload: Dict[str, Any]
    ) -> Dict[str, Any]: ...
    async def get_service(self, name: str) -> Any: ...


class DiscoveryService:
    """
    Scanne tous les plugins actifs dans XCore et indexe leurs actions IPC
    ainsi que leurs événements (listens / emits).

    Chaque plugin qui expose `xflow_integration` est automatiquement
    référencé dans le catalogue.
    """

    def __init__(self, plugin: _PluginLike) -> None:
        self.plugin = plugin
        self._registry: Dict[str, Dict[str, Any]] = {}

        # Index inversé : event_name -> liste de plugins qui l'émettent
        self._emitters_index: Dict[str, List[str]] = {}
        # Index inversé : event_name -> liste de plugins qui l'écoutent
        self._listeners_index: Dict[str, List[str]] = {}

    # ------------------------------------------------------------------
    # Scan
    # ------------------------------------------------------------------

    async def scan_all_plugins(self) -> Dict[str, Dict[str, Any]]:
        """Scanne tous les plugins actifs et reconstruit le registry."""
        logger.info("Démarrage du scan dynamique des plugins XCore...")
        try:
            res = await self.plugin.call_plugin("xcore", "plugin.list", {})
            if res.get("status", "error") != "ok":
                logger.warning(
                    "%s => xcore.plugin.list indisponible — scan annulé.", res
                )
                return self._registry

            plugins_list: List[str] = res.get("plugins", [])
            new_registry: Dict[str, Dict[str, Any]] = {}
            #plugins_list.append('xcore')
            scan_tasks = [
                self._scan_plugin(plugin_name, new_registry)
                for plugin_name in plugins_list
                if plugin_name != "xflow"
            ]
            await asyncio.gather(*scan_tasks, return_exceptions=True)

            self._registry = new_registry
            self._rebuild_event_indexes()

            action_count = sum(
                len(v.get("ipc_actions", [])) for v in self._registry.values()
            )
            emits_count = len(self._emitters_index)
            listens_count = len(self._listeners_index)

            logger.info(
                "Scan terminé : %d plugin(s), %d action(s), "
                "%d événement(s) émis, %d événement(s) écouté(s).",
                len(self._registry),
                action_count,
                emits_count,
                listens_count,
            )
            return self._registry

        except Exception as exc:
            logger.error("Erreur lors du scan des plugins : %s", exc)
            return self._registry

    async def _scan_plugin(self, plugin_name: str, registry: Dict[str, Any]) -> None:
        """Tente d'obtenir le contrat XFlow d'un plugin."""
        try:
            contract_res = await self.plugin.call_plugin(
                plugin_name, "xflow.integration", {}
            )
            if isinstance(contract_res, dict) and contract_res.get("status") == "ok":
                registry[plugin_name] = contract_res

                action_count = len(contract_res.get("ipc_actions", []))
                events = contract_res.get("events", {})
                emits = events.get("emits", [])
                listens = events.get("listens", [])

                logger.info(
                    "Plugin '%s' indexé — %d action(s), %d emit(s), %d listen(s).",
                    plugin_name,
                    action_count,
                    len(emits),
                    len(listens),
                )
        except Exception as e:
            logger.debug("Plugin '%s' sans support XFlow : %s", plugin_name, e)

    def _rebuild_event_indexes(self) -> None:
        """Reconstruit les index inversés emitters / listeners depuis le registry."""
        emitters: Dict[str, List[str]] = {}
        listeners: Dict[str, List[str]] = {}

        for plugin_name, info in self._registry.items():
            events = info.get("events", {})

            for event_name in events.get("emits", []):
                emitters.setdefault(event_name, []).append(plugin_name)

            for event_name in events.get("listens", []):
                listeners.setdefault(event_name, []).append(plugin_name)

        self._emitters_index = emitters
        self._listeners_index = listeners

    # ------------------------------------------------------------------
    # Actions IPC
    # ------------------------------------------------------------------

    def get_action_metadata(self, qualified_name: str) -> Optional[Dict[str, Any]]:
        """Retourne les métadonnées d'une action (ex: 'auth.verify.user.token')."""
        if "." not in qualified_name:
            return None
        plugin_name, action_name = qualified_name.split(".", 1)
        plugin_info = self._registry.get(plugin_name)
        if not plugin_info:
            return None
        for action in plugin_info.get("ipc_actions", []):
            if action.get("name") == action_name:
                return action
        return None

    def list_available_actions(self) -> List[Dict[str, Any]]:
        """Retourne la liste plate de toutes les actions IPC découvertes."""
        return list(self._registry.items())
        

    # ------------------------------------------------------------------
    # Événements
    # ------------------------------------------------------------------

    def get_plugin_events(self, plugin_name: str) -> Optional[Dict[str, List[str]]]:
        """
        Retourne les événements d'un plugin sous la forme :
            {"emits": [...], "listens": [...]}
        Retourne None si le plugin n'est pas dans le registry.
        """
        info = self._registry.get(plugin_name)
        if info is None:
            return None
        events = info.get("events", {})
        return {
            "emits": events.get("emits", []),
            "listens": events.get("listens", []),
        }

    def list_all_events(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Retourne un dictionnaire de tous les événements connus :
            {
                "auth.sessions.expired": {
                    "emitted_by": ["auth"],
                    "listened_by": []
                },
                "auth.get.user.ids": {
                    "emitted_by": [],
                    "listened_by": ["auth"]
                },
                ...
            }
        """
        all_event_names = set(self._emitters_index) | set(self._listeners_index)
        return {
            event: {
                "emitted_by": self._emitters_index.get(event, []),
                "listened_by": self._listeners_index.get(event, []),
            }
            for event in sorted(all_event_names)
        }

    def get_event_emitters(self, event_name: str) -> List[str]:
        """Retourne les plugins qui émettent l'événement donné."""
        return self._emitters_index.get(event_name, [])

    def get_event_listeners(self, event_name: str) -> List[str]:
        """Retourne les plugins qui écoutent l'événement donné."""
        return self._listeners_index.get(event_name, [])

    def find_event_route(self, event_name: str) -> Dict[str, List[str]]:
        """
        Pratique pour déboguer : montre qui émet et qui écoute un événement.
            {
                "event": "auth_user.loaded",
                "emitted_by": ["auth"],
                "listened_by": ["mail", "profile"]
            }
        """
        return {
            "event": event_name,
            "emitted_by": self.get_event_emitters(event_name),
            "listened_by": self.get_event_listeners(event_name),
        }

    # ------------------------------------------------------------------
    # Utilitaires
    # ------------------------------------------------------------------

    def plugin_count(self) -> int:
        return len(self._registry)

    def get_plugin_summary(self, plugin_name: str) -> Optional[Dict[str, Any]]:
        """
        Retourne un résumé complet d'un plugin :
        actions IPC + événements.
        """
        info = self._registry.get(plugin_name)
        if info is None:
            return None
        return {
            "plugin": plugin_name,
            "display_name": info.get("display_name", plugin_name),
            "description": info.get("description", ""),
            "xflow_supported": True,
            "action_count": len(info.get("ipc_actions", [])),
            "ipc_actions": [
                {
                    #"qualified_name": f"{plugin_name}.{act.get('name')}",
                    **act,
                }
                for act in info.get("ipc_actions", [])
            ],
            "events": {
                "emits": info.get("events", {}).get("emits", []),
                "listens": info.get("events", {}).get("listens", []),
            },
        }