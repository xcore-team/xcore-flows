"""
Modèles Pydantic pour la définition et l'état des workflows XFlow V2.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


class _StepBase(BaseModel):
    """Base des steps : autorise les métadonnées d'éditeur (_x, _y) à être
    conservées au round-trip sans les déclarer explicitement."""
    model_config = ConfigDict(extra="allow")


class StepType(str, Enum):
    ACTION    = "action"
    CONDITION = "condition"
    PARALLEL  = "parallel"
    WAIT      = "wait"
    SWITCH    = "switch"
    FOREACH   = "foreach"
    TRANSFORM = "transform"
    TEMPLATE  = "template"
    AI        = "ai"


class TriggerType(str, Enum):
    MANUAL   = "manual"
    EVENT    = "event"
    WEBHOOK  = "webhook"
    SCHEDULE = "schedule"


class RetryBackoff(str, Enum):
    CONSTANT    = "constant"
    LINEAR      = "linear"
    EXPONENTIAL = "exponential"


class WorkflowStatus(str, Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    SUCCESS   = "success"
    FAILED    = "failed"
    CANCELLED = "cancelled"
    PAUSED    = "paused"


class StepStatus(str, Enum):
    PENDING  = "pending"
    RUNNING  = "running"
    SUCCESS  = "success"
    FAILED   = "failed"
    SKIPPED  = "skipped"
    RETRYING = "retrying"


class ConditionOperator(str, Enum):
    EQ          = "=="
    NEQ         = "!="
    GT          = ">"
    GTE         = ">="
    LT          = "<"
    LTE         = "<="
    IN          = "in"
    NOT_IN      = "not_in"
    CONTAINS    = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH   = "ends_with"
    IS_NULL     = "is_null"
    IS_NOT_NULL = "is_not_null"
    REGEX       = "regex"


class RetryConfig(BaseModel):
    max_attempts: int = Field(default=3, ge=1, le=20)
    delay_seconds: float = Field(default=5.0, ge=0.1)
    backoff: RetryBackoff = RetryBackoff.EXPONENTIAL
    max_delay_seconds: float = Field(default=300.0)
    retry_on_codes: List[str] = Field(
        default_factory=list,
        description="Codes d'erreur spécifiques déclenchant le retry. Vide = tous.",
    )

    def compute_delay(self, attempt: int) -> float:
        if self.backoff == RetryBackoff.CONSTANT:
            delay = self.delay_seconds
        elif self.backoff == RetryBackoff.LINEAR:
            delay = self.delay_seconds * attempt
        else:  # exponential
            delay = self.delay_seconds * (2 ** (attempt - 1))
        return min(delay, self.max_delay_seconds)


class ConditionConfig(BaseModel):
    left: str = Field(description="Valeur gauche (peut contenir {{ variables }})")
    operator: ConditionOperator = ConditionOperator.EQ
    right: Optional[str] = Field(
        default=None,
        description="Valeur droite (ignorée pour is_null / is_not_null)",
    )


class TriggerConfig(_StepBase):
    type: TriggerType = TriggerType.MANUAL
    event_name: Optional[str] = None
    webhook_path: Optional[str] = None
    webhook_secret: Optional[str] = None
    cron: Optional[str] = None
    interval_seconds: Optional[int] = None
    initial_payload: Dict[str, Any] = Field(default_factory=dict)


class WebhookNotification(BaseModel):
    url: str
    method: Literal["POST", "PUT", "PATCH"] = "POST"
    headers: Dict[str, str] = Field(default_factory=dict)
    body_template: Optional[Dict[str, Any]] = None
    on_events: List[
        Literal["start", "success", "failure", "step_success", "step_failure"]
    ] = Field(default_factory=lambda: ["success", "failure"])
    timeout_seconds: float = 10.0


class ActionStep(_StepBase):
    id: str
    type: Literal[StepType.ACTION] = StepType.ACTION
    plugin: str = Field(description="Nom du plugin cible")
    action: str = Field(description="Nom de l'action IPC")
    payload: Dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: Optional[float] = None
    retry: Optional[RetryConfig] = None
    on_success: Optional[str] = Field(default=None)
    on_failure: Optional[str] = Field(default=None)
    description: Optional[str] = None


class ConditionStep(_StepBase):
    id: str
    type: Literal[StepType.CONDITION] = StepType.CONDITION
    condition: ConditionConfig
    if_true: Optional[str] = None
    if_false: Optional[str] = None
    description: Optional[str] = None


class ParallelStep(_StepBase):
    id: str
    type: Literal[StepType.PARALLEL] = StepType.PARALLEL
    branches: List[List[str]] = Field(
        description="Chaque branche est une liste d'IDs de steps"
    )
    wait_all: bool = True
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    description: Optional[str] = None


class WaitStep(_StepBase):
    id: str
    type: Literal[StepType.WAIT] = StepType.WAIT
    delay_seconds: Optional[float] = None
    wait_for_event: Optional[str] = None
    timeout_seconds: Optional[float] = None
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    description: Optional[str] = None


class SwitchStep(_StepBase):
    id: str
    type: Literal[StepType.SWITCH] = StepType.SWITCH
    expression: str = Field(description="Expression à évaluer")
    cases: Dict[str, str] = Field(description="Mapping valeur -> step_id")
    default: Optional[str] = None
    description: Optional[str] = None


class ForeachStep(_StepBase):
    id: str
    type: Literal[StepType.FOREACH] = StepType.FOREACH
    items: str = Field(description="Expression pointant vers la liste à itérer")
    workflow_name: Optional[str] = Field(None, description="Workflow à appeler pour chaque item")
    steps: Optional[List[str]] = Field(None, description="IDs de steps à exécuter en boucle")
    parallel: bool = False
    max_parallel: int = 5
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    description: Optional[str] = None


class TransformStep(_StepBase):
    id: str
    type: Literal[StepType.TRANSFORM] = StepType.TRANSFORM
    query: str = Field(description="Requête JQ ou expression de transformation")
    input: Optional[str] = None
    on_success: Optional[str] = None
    description: Optional[str] = None


class TemplateStep(_StepBase):
    id: str
    type: Literal[StepType.TEMPLATE] = StepType.TEMPLATE
    template: str = Field(description="Template Jinja2")
    output_key: str = "rendered"
    on_success: Optional[str] = None
    description: Optional[str] = None


class AIService(str, Enum):
    SUMMARIZE = "summarize"
    CLASSIFY  = "classify"
    DECIDE    = "decide"
    EXTRACT   = "extract"


class AIStep(_StepBase):
    id: str
    type: Literal[StepType.AI] = StepType.AI
    service: AIService
    prompt: str
    context: Optional[str] = None
    options: Dict[str, Any] = Field(default_factory=dict)
    on_success: Optional[str] = None
    on_failure: Optional[str] = None
    description: Optional[str] = None


AnyStep = Union[
    ActionStep,
    ConditionStep,
    ParallelStep,
    WaitStep,
    SwitchStep,
    ForeachStep,
    TransformStep,
    TemplateStep,
    AIStep,
]

_STEP_TYPE_MAP = {
    StepType.ACTION:    ActionStep,
    StepType.CONDITION: ConditionStep,
    StepType.PARALLEL:  ParallelStep,
    StepType.WAIT:      WaitStep,
    StepType.SWITCH:    SwitchStep,
    StepType.FOREACH:   ForeachStep,
    StepType.TRANSFORM: TransformStep,
    StepType.TEMPLATE:  TemplateStep,
    StepType.AI:        AIStep,
}


class WorkflowDefinition(BaseModel):
    name: str = Field(description="Identifiant unique du workflow")
    version: str = "1.0.0"
    description: Optional[str] = None
    trigger: TriggerConfig = Field(default_factory=TriggerConfig)
    steps: List[AnyStep] = Field(min_length=1)
    entry_step: Optional[str] = Field(
        default=None,
        description="ID du step de départ. Par défaut : premier step.",
    )
    webhooks: List[WebhookNotification] = Field(default_factory=list)
    timeout_seconds: Optional[float] = None
    tags: List[str] = Field(default_factory=list)

    @field_validator("steps", mode="before")
    @classmethod
    def validate_steps(cls, steps: list) -> list:
        parsed = []
        for step in steps:
            if isinstance(step, dict):
                raw_type = step.get("type", StepType.ACTION)
                step_type = StepType(raw_type) if isinstance(raw_type, str) else raw_type
                klass = _STEP_TYPE_MAP.get(step_type)
                if klass is None:
                    raise ValueError(f"Type de step inconnu : {raw_type}")
                parsed.append(klass(**step))
            else:
                parsed.append(step)
        return parsed

    def get_step(self, step_id: str) -> Optional[AnyStep]:
        for step in self.steps:
            if step.id == step_id:
                return step
        return None

    @property
    def start_step_id(self) -> str:
        return self.entry_step or self.steps[0].id

    def export_graph(self) -> Dict[str, Any]:
        """Exporte le workflow sous forme nodes/edges pour visualisation."""
        nodes, edges = [], []
        for step in self.steps:
            nodes.append({
                "id": step.id,
                "type": step.type.value,
                "label": getattr(step, "description", None) or step.id,
                "data": step.model_dump(exclude={"id", "type", "description"}),
            })
            for attr, label in [("on_success", "success"), ("on_failure", "failure")]:
                target = getattr(step, attr, None)
                if target:
                    edges.append({"source": step.id, "target": target, "label": label})
            if step.type == StepType.CONDITION:
                if step.if_true:
                    edges.append({"source": step.id, "target": step.if_true, "label": "true"})
                if step.if_false:
                    edges.append({"source": step.id, "target": step.if_false, "label": "false"})
            if step.type == StepType.SWITCH:
                for val, target in step.cases.items():
                    edges.append({"source": step.id, "target": target, "label": val})
                if step.default:
                    edges.append({"source": step.id, "target": step.default, "label": "default"})
            if step.type == StepType.PARALLEL:
                for branch in step.branches:
                    if branch:
                        edges.append({"source": step.id, "target": branch[0], "label": "branch"})
        return {"nodes": nodes, "edges": edges}


class StepRun(BaseModel):
    step_id: str
    status: StepStatus = StepStatus.PENDING
    attempt: int = 0
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class WorkflowRun(BaseModel):
    run_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    tenant_id: str = "default"
    workflow_name: str
    workflow_version: str = "1.0.0"
    status: WorkflowStatus = WorkflowStatus.PENDING
    trigger_payload: Dict[str, Any] = Field(default_factory=dict)
    context: Dict[str, Any] = Field(default_factory=dict)
    steps: Dict[str, StepRun] = Field(default_factory=dict)
    current_step_id: Optional[str] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    error: Optional[str] = None
    scheduled_job_id: Optional[str] = None
