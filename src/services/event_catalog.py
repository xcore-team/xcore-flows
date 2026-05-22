"""
Service de catalogue d'événements — XFlow V2.

Découvre et indexe tous les événements émis dans le système XCore
via l'introspection de l'EventBus.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol

from ..schemas.events import EventCatalogEntry, EventSchema

logger = logging.getLogger("xflow.event_catalog")


class EventBusLike(Protocol):
    """Protocol pour l'EventBus XCore."""

    def list_events(self) -> Dict[str, List[Any]]: ...


class EventCatalogService:
    """
    Service de découverte et catalogue des événements.

    Les événements ne sont pas enregistrés centralement dans XCore —
    ils sont émis de manière ad-hoc. Ce service maintient un catalogue
    basé sur:
    1. L'historique des événements émis
    2. Les événements déclarés dans les workflows existants
    3. La documentation des plugins (si disponible)
    """

    def __init__(self, ctx: Any) -> None:
        """
        Initialise le service.

        Args:
            ctx: Plugin context avec accès à l'EventBus
        """
        self._ctx = ctx
        self._catalog: Dict[str, EventSchema] = {}
        self._event_counts: Dict[str, int] = {}

    async def discover_events(self) -> Dict[str, EventSchema]:
        """
        Découvre les événements disponibles via introspection.

        Méthodes de découverte:
        1. Interroger l'EventBus pour les handlers enregistrés
        2. Scanner les workflows pour les triggers de type 'event'
        3. Appeler les plugins pour leur documentation d'événements
        """
        discovered: Dict[str, EventSchema] = {}

        # 1. Découvrir via l'EventBus
        try:
            event_handlers = self._ctx.events.list_events()
            for event_name, handlers in event_handlers.items():
                if event_name not in discovered:
                    discovered[event_name] = EventSchema(
                        name=event_name,
                        description=f"Événement EventBus avec {len(handlers)} handler(s)",
                        source_plugin=None,
                        payload_schema={},
                    )
        except Exception as e:
            logger.warning("Erreur lors de la découverte EventBus: %s", e)

        # 2. Enrichir via les plugins
        try:
            # Appeler xcore.plugin.list pour obtenir les plugins
            res = await self._ctx.ipc.call("xcore", "plugin.list", {})
            if res.get("status") == "ok":
                plugins = res.get("plugins", [])
                for plugin_name in plugins:
                    plugin_events = await self._discover_plugin_events(plugin_name)
                    for event in plugin_events:
                        discovered[event.name] = event
        except Exception as e:
            logger.debug("Erreur découverte plugins: %s", e)

        self._catalog = discovered
        logger.info(
            "Catalogue événements: %d événement(s) découvert(s).",
            len(discovered),
        )
        return discovered

    async def _discover_plugin_events(
        self, plugin_name: str
    ) -> List[EventSchema]:
        """
        Interroge un plugin pour sa documentation d'événements.

        Les plugins peuvent exposer une action `events.list` qui retourne
        la liste des événements qu'ils émettent.
        """
        events = []
        try:
            res = await self._ctx.ipc.call(plugin_name, "events.list", {})
            if res.get("status") == "ok":
                data = res.get("data", {}).get("events", [])
                for event_data in data:
                    events.append(
                        EventSchema(
                            name=event_data.get("name", ""),
                            description=event_data.get("description"),
                            source_plugin=plugin_name,
                            payload_schema=event_data.get("payload_schema", {}),
                            example=event_data.get("example"),
                        )
                    )
        except Exception:
            # Plugin ne supporte pas events.list
            pass
        return events

    def list_events(self) -> List[EventCatalogEntry]:
        """Retourne la liste des événements catalogués."""
        entries = []
        for event in self._catalog.values():
            entries.append(
                EventCatalogEntry(
                    name=event.name,
                    description=event.description,
                    source_plugin=event.source_plugin,
                    payload_schema=event.payload_schema,
                    example=event.example,
                    workflow_count=self._event_counts.get(event.name, 0),
                )
            )
        return entries

    def get_event(self, name: str) -> Optional[EventSchema]:
        """Récupère les détails d'un événement."""
        return self._catalog.get(name)

    def register_workflow_trigger(self, event_name: str) -> None:
        """Incrémente le compteur de workflows pour un événement."""
        self._event_counts[event_name] = self._event_counts.get(event_name, 0) + 1

    async def refresh(self) -> None:
        """Rafraîchit le catalogue."""
        await self.discover_events()
