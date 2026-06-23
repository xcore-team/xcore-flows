from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from sqlalchemy import Column, DateTime, String, Text, JSON, Boolean, ForeignKey, Integer, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class FlowRecord(Base):
    __tablename__ = "xflow_flows"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_xflow_flows_tenant_name"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    name: Mapped[str] = mapped_column(String(255), index=True)
    description: Mapped[Optional[str]] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, onupdate=_utcnow)

    versions: Mapped[list["FlowVersionRecord"]] = relationship(back_populates="flow", cascade="all, delete-orphan")


class FlowVersionRecord(Base):
    __tablename__ = "xflow_versions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_id: Mapped[int] = mapped_column(ForeignKey("xflow_flows.id"))
    version_tag: Mapped[str] = mapped_column(String(64), default="1.0.0")
    definition: Mapped[Dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    flow: Mapped["FlowRecord"] = relationship(back_populates="versions")
    runs: Mapped[list["FlowRunRecord"]] = relationship(back_populates="version")


class FlowRunRecord(Base):
    __tablename__ = "xflow_runs"

    run_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    version_id: Mapped[Optional[int]] = mapped_column(ForeignKey("xflow_versions.id"), nullable=True)
    status: Mapped[str] = mapped_column(String(32), index=True)
    trigger_type: Mapped[str] = mapped_column(String(32))
    trigger_data: Mapped[Dict[str, Any]] = mapped_column(JSON)
    context: Mapped[Dict[str, Any]] = mapped_column(JSON, default=dict)
    error: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)

    version: Mapped[Optional["FlowVersionRecord"]] = relationship(back_populates="runs")
    steps: Mapped[list["FlowStepRecord"]] = relationship(back_populates="run", cascade="all, delete-orphan")


class FlowStepRecord(Base):
    __tablename__ = "xflow_steps"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("xflow_runs.run_id"))
    step_id: Mapped[str] = mapped_column(String(255))
    step_type: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(32))
    input_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    output_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    error: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    run: Mapped["FlowRunRecord"] = relationship(back_populates="steps")


class FlowScheduleRecord(Base):
    __tablename__ = "xflow_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    flow_id: Mapped[int] = mapped_column(ForeignKey("xflow_flows.id"))
    cron: Mapped[str] = mapped_column(String(128))
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    next_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class FlowDeadJobRecord(Base):
    __tablename__ = "xflow_dead_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64))
    payload: Mapped[Dict[str, Any]] = mapped_column(JSON)
    error: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class FlowAuditLogRecord(Base):
    __tablename__ = "xflow_audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    level: Mapped[str] = mapped_column(String(16))
    message: Mapped[str] = mapped_column(Text)
    extra_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class CompositeRecord(Base):
    __tablename__ = "xflow_composites"
    __table_args__ = (UniqueConstraint("tenant_id", "name", name="uq_xflow_composites_tenant_name"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    tenant_id = Column(String(64), nullable=False, index=True)
    name = Column(String(255), nullable=False, index=True)
    version = Column(String(64), default="1.0.0")
    description = Column(Text)
    icon = Column(String(64))
    category = Column(String(64), default="custom")
    definition = Column(JSON, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
