"""first_migration — ajout tenant_id sur xflow_flows et xflow_runs

Revision ID: 952584a8d052
Revises:
Create Date: 2026-06-22 19:25:38.698765

Ajoute la colonne tenant_id (multi-tenant) sur xflow_flows et xflow_runs.
Supprime l'ancienne table xflow_composites (sans tenant_id) — recrée par
la migration suivante (b3f1c2e4d891) avec le nouveau schéma.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembics import op

revision: str = '952584a8d052'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Supprimer l'ancienne xflow_composites sans tenant_id (si elle existe)
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = inspector.get_table_names()

    if 'xflow_composites' in existing:
        op.drop_index(op.f('ix_xflow_composites_name'), table_name='xflow_composites', if_exists=True)
        op.drop_table('xflow_composites')

    # Ajouter tenant_id à xflow_flows
    if 'xflow_flows' in existing:
        cols = [c['name'] for c in inspector.get_columns('xflow_flows')]
        if 'tenant_id' not in cols:
            op.add_column('xflow_flows', sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'))
            op.create_index(op.f('ix_xflow_flows_tenant_id'), 'xflow_flows', ['tenant_id'], unique=False)
            op.create_unique_constraint('uq_xflow_flows_tenant_name', 'xflow_flows', ['tenant_id', 'name'])

    # Ajouter tenant_id à xflow_runs
    if 'xflow_runs' in existing:
        cols = [c['name'] for c in inspector.get_columns('xflow_runs')]
        if 'tenant_id' not in cols:
            op.add_column('xflow_runs', sa.Column('tenant_id', sa.String(64), nullable=False, server_default='default'))
            op.create_index(op.f('ix_xflow_runs_tenant_id'), 'xflow_runs', ['tenant_id'], unique=False)


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing = inspector.get_table_names()

    if 'xflow_runs' in existing:
        cols = [c['name'] for c in inspector.get_columns('xflow_runs')]
        if 'tenant_id' in cols:
            op.drop_index(op.f('ix_xflow_runs_tenant_id'), table_name='xflow_runs')
            op.drop_column('xflow_runs', 'tenant_id')

    if 'xflow_flows' in existing:
        cols = [c['name'] for c in inspector.get_columns('xflow_flows')]
        if 'tenant_id' in cols:
            op.drop_constraint('uq_xflow_flows_tenant_name', 'xflow_flows', type_='unique')
            op.drop_index(op.f('ix_xflow_flows_tenant_id'), table_name='xflow_flows')
            op.drop_column('xflow_flows', 'tenant_id')

    # Recréer l'ancienne xflow_composites sans tenant_id
    op.create_table(
        'xflow_composites',
        sa.Column('id', sa.Integer(), nullable=False, autoincrement=True),
        sa.Column('name', sa.String(255), nullable=False),
        sa.Column('version', sa.String(64), nullable=True),
        sa.Column('description', sa.Text(), nullable=True),
        sa.Column('icon', sa.String(64), nullable=True),
        sa.Column('category', sa.String(64), nullable=True),
        sa.Column('definition', sa.JSON(), nullable=False),
        sa.Column('is_active', sa.Boolean(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_xflow_composites_name'), 'xflow_composites', ['name'], unique=True)
