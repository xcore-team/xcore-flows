"""
Service de catalogue d'événements — XFlow V2.

Source primaire : DiscoveryService (schemas.json → events_emitted déclarés dans plugin.yaml).
Source secondaire : EventBus (handlers enregistrés dynamiquement au runtime).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ..schemas.events import EventCatalogEntry, EventSchema
from .discovery import DiscoveryService

logger = logging.getLogger("xflow.event_catalog")


class EventCatalogService:
    """
    Catalogue des événements disponibles dans xcore.

    Alimenté statiquement depuis DiscoveryService (events_emitted de schemas.json)
    et enrichi dynamiquement par l'EventBus pour les abonnements non déclarés.
    """

    def __init__(self, discovery: DiscoveryService, ctx: Any | None = None) -> None:
        self._discovery = discovery
        self._ctx = ctx
        self._catalog: Dict[str, EventSchema] = {}
        self._event_counts: Dict[str, int] = {}
        self._load_from_discovery()

    def _load_from_discovery(self) -> None:
        """Charge le catalogue depuis les events_emitted déclarés statiquement."""
        for evt in self._discovery.list_emitted_events():
            self._catalog[evt["name"]] = EventSchema(
                name=evt["name"],
                description=evt.get("description", ""),
                source_plugin=evt.get("source_plugin"),
                payload_schema=evt.get("payload_schema", {}),
            )
        logger.info(
            "Catalogue événements chargé depuis discovery — %d événement(s).",
            len(self._catalog),
        )

    async def refresh(self) -> None:
        """
        Rafraîchit le catalogue.
        Recharge depuis discovery (statique) puis enrichit via EventBus (dynamique).
        """
        self._load_from_discovery()
        await self._enrich_from_eventbus()

    async def _enrich_from_eventbus(self) -> None:
        """
        Ajoute au catalogue les événements découverts sur l'EventBus
        qui ne sont pas encore déclarés dans schemas.json.
        """
        if not self._ctx:
            return
        try:
            event_handlers = self._ctx.events.list_events()
            for event_name, handlers in event_handlers.items():
                if event_name not in self._catalog:
                    self._catalog[event_name] = EventSchema(
                        name=event_name,
                        description=f"Événement EventBus avec {len(handlers)} handler(s)",
                        source_plugin=None,
                        payload_schema={},
                    )
        except Exception as exc:
            logger.debug("EventBus.list_events() indisponible : %s", exc)

    def list_events(self) -> List[EventCatalogEntry]:
        """Retourne la liste des événements catalogués."""
        return [
            EventCatalogEntry(
                name=evt.name,
                description=evt.description,
                source_plugin=evt.source_plugin,
                payload_schema=evt.payload_schema,
                example=evt.example,
                workflow_count=self._event_counts.get(evt.name, 0),
            )
            for evt in self._catalog.values()
        ]

    def get_event(self, name: str) -> Optional[EventSchema]:
        """Récupère les détails d'un événement."""
        return self._catalog.get(name)

    def register_workflow_trigger(self, event_name: str) -> None:
        """Incrémente le compteur de workflows pour un événement."""
        self._event_counts[event_name] = self._event_counts.get(event_name, 0) + 1
