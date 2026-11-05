"""
Authentification des webhooks entrants — XFlow.

Contrat : chaque workflow à déclencheur webhook possède un secret propre.
L'appelant signe `"{timestamp}." + corps_brut` en HMAC-SHA256 avec ce secret et envoie :

    X-XFlow-Timestamp: <secondes unix>
    X-XFlow-Signature: sha256=<hex>

Le serveur retrouve le secret par (tenant_id, workflow) ; le run s'exécute dans le tenant
porté par l'enregistrement du secret, jamais dans la valeur brute de l'URL.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import os
import secrets
import time
from collections import OrderedDict, deque
from datetime import datetime, timezone
from typing import Any, Deque, Dict, Optional

from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError

from ..repositories.models import WebhookSecretRecord

logger = logging.getLogger("xflow.webhook_auth")

SIGNATURE_HEADER = "X-XFlow-Signature"
TIMESTAMP_HEADER = "X-XFlow-Timestamp"
TOLERANCE_SECONDS = 300
MAX_BODY_BYTES = 1024 * 1024
KEK_ENV = "XFLOW_WEBHOOK_KEK"

# Secret factice pour égaliser le temps de réponse quand aucun secret n'existe.
_DUMMY_SECRET = secrets.token_urlsafe(32)


def generate_secret() -> str:
    return "whsec_" + secrets.token_urlsafe(32)


def sign(secret: str, timestamp: str, body: bytes) -> str:
    """Hex HMAC-SHA256 de `"{timestamp}." + body`."""
    msg = timestamp.encode() + b"." + body
    return hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()


def verify(
    secret: str,
    timestamp: Optional[str],
    signature: Optional[str],
    body: bytes,
    now: Optional[float] = None,
    tolerance: int = TOLERANCE_SECONDS,
) -> bool:
    """Vérifie signature + fenêtre d'horodatage (comparaison à temps constant)."""
    if not timestamp or not signature:
        return False
    try:
        ts = int(timestamp)
    except ValueError:
        return False
    now = time.time() if now is None else now
    if abs(now - ts) > tolerance:
        return False
    provided = signature.removeprefix("sha256=")
    return hmac.compare_digest(provided.encode(), sign(secret, timestamp, body).encode())


def verify_dummy(timestamp: Optional[str], signature: Optional[str], body: bytes) -> None:
    """Travail équivalent quand le workflow/secret n'existe pas (anti oracle temporel)."""
    verify(_DUMMY_SECRET, timestamp, signature, body)


class WebhookGuard:
    """Limiteur de débit par client + mémoire anti-rejeu, en processus (best effort).

    Avec plusieurs workers, chaque processus a sa propre mémoire : la fenêtre
    d'horodatage reste la protection de fond contre le rejeu.
    """

    def __init__(self, max_per_minute: int = 120, max_keys: int = 10_000) -> None:
        self.max_per_minute = max_per_minute
        self._max_keys = max_keys
        self._hits: "OrderedDict[str, Deque[float]]" = OrderedDict()
        self._seen: Dict[str, float] = {}

    def allow(self, client: str) -> bool:
        now = time.monotonic()
        q = self._hits.get(client)
        if q is None:
            if len(self._hits) >= self._max_keys:
                self._hits.popitem(last=False)
            q = self._hits[client] = deque()
        else:
            self._hits.move_to_end(client)
        while q and now - q[0] > 60:
            q.popleft()
        if len(q) >= self.max_per_minute:
            return False
        q.append(now)
        return True

    def first_use(self, signature_id: str) -> bool:
        """True si cette signature n'a pas encore été vue dans la fenêtre."""
        now = time.monotonic()
        if len(self._seen) > 50_000:
            self._seen = {k: t for k, t in self._seen.items() if now - t < 2 * TOLERANCE_SECONDS}
        t = self._seen.get(signature_id)
        if t is not None and now - t < 2 * TOLERANCE_SECONDS:
            return False
        self._seen[signature_id] = now
        return True


def _fernet() -> Any:
    raw = os.environ.get(KEK_ENV)
    if not raw:
        return None
    from cryptography.fernet import Fernet

    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(raw.encode()).digest()))


def _seal(secret: str) -> str:
    f = _fernet()
    if f is None:
        return "plain:" + secret
    return "enc:" + f.encrypt(secret.encode()).decode()


def _unseal(stored: str) -> Optional[str]:
    if stored.startswith("plain:"):
        return stored[6:]
    if stored.startswith("enc:"):
        f = _fernet()
        if f is None:
            logger.error("Secret webhook chiffré mais %s absent.", KEK_ENV)
            return None
        try:
            return f.decrypt(stored[4:].encode()).decode()
        except Exception:
            logger.error("Secret webhook indéchiffrable (clé %s modifiée ?).", KEK_ENV)
            return None
    return None


class WebhookSecretStore:
    """Secrets de signature par (tenant, workflow). Jamais renvoyés par une lecture."""

    def __init__(self, db: Any) -> None:
        self._db = db

    async def get(self, tenant_id: str, workflow: str) -> Optional[str]:
        async with self._db.session() as s:
            row = (await s.execute(self._q(tenant_id, workflow))).scalar_one_or_none()
        return _unseal(row.secret_value) if row else None

    async def status(self, tenant_id: str, workflow: str) -> Optional[Dict[str, Any]]:
        async with self._db.session() as s:
            row = (await s.execute(self._q(tenant_id, workflow))).scalar_one_or_none()
        if row is None:
            return None
        return {
            "created_at": row.created_at.isoformat() if row.created_at else None,
            "rotated_at": row.rotated_at.isoformat() if row.rotated_at else None,
        }

    async def ensure(self, tenant_id: str, workflow: str) -> Optional[str]:
        """Crée le secret s'il n'existe pas ; le retourne (en clair) uniquement à la création."""
        secret = generate_secret()
        try:
            async with self._db.session() as s:
                row = (await s.execute(self._q(tenant_id, workflow))).scalar_one_or_none()
                if row is not None:
                    return None
                s.add(WebhookSecretRecord(
                    tenant_id=tenant_id, workflow_name=workflow, secret_value=_seal(secret),
                ))
                await s.commit()
        except IntegrityError:
            return None
        return secret

    async def rotate(self, tenant_id: str, workflow: str) -> str:
        """(Re)génère le secret ; l'ancien cesse immédiatement de fonctionner."""
        secret = generate_secret()
        async with self._db.session() as s:
            row = (await s.execute(self._q(tenant_id, workflow))).scalar_one_or_none()
            if row is None:
                s.add(WebhookSecretRecord(
                    tenant_id=tenant_id, workflow_name=workflow, secret_value=_seal(secret),
                ))
            else:
                row.secret_value = _seal(secret)
                row.rotated_at = datetime.now(timezone.utc)
            await s.commit()
        return secret

    async def delete(self, tenant_id: str, workflow: str) -> None:
        async with self._db.session() as s:
            await s.execute(
                delete(WebhookSecretRecord).where(
                    WebhookSecretRecord.tenant_id == tenant_id,
                    WebhookSecretRecord.workflow_name == workflow,
                )
            )
            await s.commit()

    @staticmethod
    def _q(tenant_id: str, workflow: str):
        return select(WebhookSecretRecord).where(
            WebhookSecretRecord.tenant_id == tenant_id,
            WebhookSecretRecord.workflow_name == workflow,
        )
