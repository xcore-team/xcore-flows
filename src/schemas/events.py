"""
Schémas Pydantic pour le catalogue d'événements XFlow V2.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class EventSchema(BaseModel):
    """Schema d'un événement disponible."""
    name: str = Field(description="Nom qualifié de l'événement (ex: user.created)")
    description: Optional[str] = Field(default=None)
    source_plugin: Optional[str] = Field(default=None, description="Plugin qui émet l'événement")
    payload_schema: Dict[str, Any] = Field(
        default_factory=dict,
        description="Schema JSON des données de l'événement"
    )
    example: Optional[Dict[str, Any]] = Field(default=None, description="Exemple de payload")


class EventCatalogEntry(BaseModel):
    """Entrée dans le catalogue d'événements."""
    name: str
    description: Optional[str]
    source_plugin: Optional[str]
    payload_schema: Dict[str, Any]
    example: Optional[Dict[str, Any]]
    workflow_count: int = Field(
        default=0,
        description="Nombre de workflows écoutant cet événement"
    )


class EventTriggerConfig(BaseModel):
    """Configuration pour un trigger basé sur un événement."""
    type: str = "event"
    event_name: str = Field(description="Nom de l'événement à écouter")
    event_names: Optional[List[str]] = Field(
        default=None,
        description="Liste d'événements (OR logic)"
    )
    filter: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Filtres sur le payload (correspondance exacte)"
    )
