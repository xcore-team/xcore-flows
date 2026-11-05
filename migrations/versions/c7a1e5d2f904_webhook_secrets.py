"""webhook_secrets — secrets de signature des webhooks entrants

Revision ID: c7a1e5d2f904
Revises: 952584a8d052
Create Date: 2026-10-14 10:00:00

Crée xflow_webhook_secrets (idempotent : no-op si la table existe déjà, par exemple
créée par Base.metadata.create_all au démarrage). Aucun secret n'est généré ici :
les workflows webhook existants restent désactivés (404) jusqu'à ce que leur
propriétaire génère un secret (redéploiement ou POST /flows/{name}/webhook/secret).
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'c7a1e5d2f904'
down_revision: Union[str, Sequence[str], None] = '952584a8d052'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if 'xflow_webhook_secrets' in inspector.get_table_names():
        return
    op.create_table(
        'xflow_webhook_secrets',
        sa.Column('id', sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column('tenant_id', sa.String(64), nullable=False),
        sa.Column('workflow_name', sa.String(255), nullable=False),
        sa.Column('secret_value', sa.Text(), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('rotated_at', sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint('tenant_id', 'workflow_name', name='uq_xflow_webhook_secrets'),
    )
    op.create_index('ix_xflow_webhook_secrets_tenant_id', 'xflow_webhook_secrets', ['tenant_id'])


def downgrade() -> None:
    inspector = sa.inspect(op.get_bind())
    if 'xflow_webhook_secrets' in inspector.get_table_names():
        op.drop_table('xflow_webhook_secrets')
