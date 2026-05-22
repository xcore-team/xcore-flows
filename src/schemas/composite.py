"""
Schémas Pydantic pour les noeuds composites XFlow V2.

Un composite node permet de regrouper plusieurs steps en une seule unité réutilisable.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .workflow import AnyStep, WorkflowDefinition


class CompositeInput(BaseModel):
    """Définition d'une entrée pour un composite node."""
    name: str = Field(description="Nom de l'entrée (utilisé dans le template)")
    description: Optional[str] = None
    required: bool = True
    default: Optional[Any] = None


class CompositeOutput(BaseModel):
    """Définition d'une sortie pour un composite node."""
    name: str = Field(description="Nom de la sortie")
    description: Optional[str] = None
    source_step: str = Field(description="ID du step qui produit cette sortie")
    source_field: str = Field(description="Champ dans le résultat du step")


class CompositeNodeDefinition(BaseModel):
    """
    Définition d'un composite node réutilisable.

    Un composite encapsule plusieurs steps et expose:
    - Des entrées (inputs) mappées vers les steps internes
    - Des sorties (outputs) agrégées depuis les résultats des steps
    """
    name: str = Field(description="Identifiant unique du composite")
    version: str = "1.0.0"
    description: Optional[str] = None
    icon: Optional[str] = Field(default=None, description="Nom d'icône (lucide-react)")
    category: Optional[str] = Field(default="custom", description="Catégorie dans la palette")

    # Steps internes du composite
    steps: List[AnyStep] = Field(min_length=1)

    # Entrées/sorties du composite
    inputs: List[CompositeInput] = Field(default_factory=list)
    outputs: List[CompositeOutput] = Field(default_factory=list)

    # Mapping des entrées vers les steps internes
    input_mappings: Dict[str, Dict[str, str]] = Field(
        default_factory=dict,
        description="Map input_name -> {step_id: '...', field: '...'}"
    )

    tags: List[str] = Field(default_factory=list)


class CompositeInstanceStep(BaseModel):
    """
    Représentation d'un composite node dans un workflow.

    Quand un composite est utilisé dans un workflow, il apparaît comme
    un seul node visuel, mais s'étend à l'exécution.
    """
    id: str
    type: str = "composite"
    composite_name: str = Field(description="Référence au composite défini")
    inputs: Dict[str, Any] = Field(
        default_factory=dict,
        description="Valeurs fournies pour les inputs du composite"
    )
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    description: Optional[str] = None


class CompositeRegistryEntry(BaseModel):
    """Entrée dans le registry des composites."""
    name: str
    version: str
    description: Optional[str]
    icon: Optional[str]
    category: str
    inputs: List[CompositeInput]
    outputs: List[CompositeOutput]
    step_count: int = Field(description="Nombre de steps encapsulés")
    tags: List[str]
