"""
Service de registre des composite nodes — XFlow V2.

Permet de créer, sauvegarder et réutiliser des noeuds composites
qui encapsulent plusieurs steps en une seule unité visuelle.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import Column, DateTime, String, Text, JSON, Boolean, Integer, select

from ..repositories import Base
from ..schemas.composite import (
    CompositeInput,
    CompositeNodeDefinition,
    CompositeOutput,
    CompositeRegistryEntry,
)
from ..schemas.workflow import StepType, WorkflowDefinition

logger = logging.getLogger("xflow.composites")


class CompositeRecord(Base):
    """Table SQLAlchemy pour stocker les composites."""
    __tablename__ = "xflow_composites"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False, index=True)
    version = Column(String(64), default="1.0.0")
    description = Column(Text)
    icon = Column(String(64))
    category = Column(String(64), default="custom")
    definition = Column(JSON, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))


class CompositeService:
    """
    Service CRUD pour les composite nodes.

    Les composites sont stockés en DB et exposés via API REST.
    À l'exécution, le moteur XFlow les "désemballe" pour exécuter
    les steps internes.
    """

    def __init__(self, db: Any) -> None:
        self._db = db

    async def create(self, definition: CompositeNodeDefinition) -> CompositeNodeDefinition:
        """Enregistre un nouveau composite."""
        from datetime import datetime, timezone

        payload = definition.model_dump(mode="json")

        async with self._db.session() as session:
            # Vérifier si le composite existe déjà
            result = await session.execute(
                select(CompositeRecord).where(CompositeRecord.name == definition.name)
            )
            existing = result.scalar_one_or_none()

            if existing:
                # Mettre à jour la version
                definition.version = self._bump_version(existing.version)
                payload["version"] = definition.version
                logger.info(
                    "Composite '%s' mis à jour vers la version %s",
                    definition.name,
                    definition.version,
                )

            record = CompositeRecord(
                name=definition.name,
                version=definition.version,
                description=definition.description,
                icon=definition.icon,
                category=definition.category,
                definition=payload,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )

            if existing:
                record.id = existing.id
                record.created_at = existing.created_at
                session.add(record)
            else:
                session.add(record)

            await session.commit()

        logger.info("Composite '%s' v%s enregistré.", definition.name, definition.version)
        return definition

    async def get(self, name: str) -> Optional[CompositeNodeDefinition]:
        """Récupère un composite par son nom."""
        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord)
                .where(CompositeRecord.name == name)
                .where(CompositeRecord.is_active == True)
            )
            record = result.scalar_one_or_none()

            if record is None:
                return None

            return CompositeNodeDefinition.model_validate(record.definition)

    async def list_all(self) -> List[CompositeRegistryEntry]:
        """Liste tous les composites disponibles."""
        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord)
                .where(CompositeRecord.is_active == True)
                .order_by(CompositeRecord.name)
            )
            records = result.scalars().all()

        entries = []
        for r in records:
            definition = CompositeNodeDefinition.model_validate(r.definition)
            entries.append(
                CompositeRegistryEntry(
                    name=r.name,
                    version=r.version,
                    description=r.description,
                    icon=r.icon,
                    category=r.category,
                    inputs=definition.inputs,
                    outputs=definition.outputs,
                    step_count=len(definition.steps),
                    tags=definition.tags,
                )
            )
        return entries

    async def delete(self, name: str) -> bool:
        """Supprime (désactive) un composite."""
        async with self._db.session() as session:
            result = await session.execute(
                select(CompositeRecord).where(CompositeRecord.name == name)
            )
            record = result.scalar_one_or_none()

            if record is None:
                return False

            record.is_active = False
            from datetime import datetime, timezone
            record.updated_at = datetime.now(timezone.utc)
            await session.commit()

        logger.info("Composite '%s' désactivé.", name)
        return True

    async def expand_composite(
        self,
        composite_name: str,
        instance_id: str,
        inputs: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Étend un composite en ses steps internes pour l'exécution.

        Retourne un dictionnaire avec:
        - steps: Liste des steps à exécuter
        - entry_step: ID du premier step
        - exit_mapping: Comment mapper les résultats vers l'extérieur
        """
        composite = await self.get(composite_name)
        if composite is None:
            return None

        # Mapper les inputs vers les steps
        expanded_steps = []
        step_remap: Dict[str, str] = {}

        for step in composite.steps:
            # Créer une copie du step avec ID préfixé
            new_id = f"{instance_id}_{step.id}"
            step_remap[step.id] = new_id

            step_data = step.model_dump(mode="json")
            step_data["id"] = new_id

            # Appliquer les mappings d'inputs
            if step.id in composite.input_mappings:
                mapping = composite.input_mappings[step.id]
                for input_name, target_field in mapping.items():
                    if input_name in inputs:
                        # Injecter l'input dans le payload du step
                        if "payload" not in step_data:
                            step_data["payload"] = {}
                        step_data["payload"][target_field] = inputs[input_name]

            # Remapper les on_success/on_failure
            if step_data.get("on_success"):
                step_data["on_success"] = f"{instance_id}_{step_data['on_success']}"
            if step_data.get("on_failure"):
                step_data["on_failure"] = f"{instance_id}_{step_data['on_failure']}"

            expanded_steps.append(step_data)

        # Calculer le step d'entrée
        entry_step = f"{instance_id}_{composite.steps[0].id}" if composite.steps else None

        # Calculer les outputs
        output_mapping = {}
        for out in composite.outputs:
            output_mapping[out.name] = {
                "step_id": f"{instance_id}_{out.source_step}",
                "field": out.source_field,
            }

        return {
            "steps": expanded_steps,
            "entry_step": entry_step,
            "output_mapping": output_mapping,
        }

    def _bump_version(self, version: str) -> str:
        """Incrémente la version mineure."""
        parts = version.split(".")
        if len(parts) >= 2:
            try:
                minor = int(parts[1])
                parts[1] = str(minor + 1)
                return ".".join(parts)
            except ValueError:
                pass
        return version + ".1"
