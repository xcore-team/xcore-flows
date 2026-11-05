# Sécurité — XFlow V2

## Modèle de confiance

XFlow est un plugin **Trusted** dans XCore. Il dispose des permissions suivantes :
- `xflow.*` — toutes les actions XFlow
- `db.*` — accès complet à la base de données
- `cache.*` — accès complet au cache
- `scheduler.*` — gestion des jobs schedulés
- `plugins.*` — appel de n'importe quel plugin

## Authentification des webhooks entrants

`POST /webhook/{workflow}?tenant_id=...` exige une signature HMAC-SHA256 propre au workflow.

- Le secret est généré au déploiement d'un workflow `trigger.type = webhook` et renvoyé **une seule fois**
  (`webhook_secret`). Régénération : `POST /flows/{name}/webhook/secret` (l'ancien est invalidé).
  Statut sans secret : `GET /flows/{name}/webhook`.
- En-têtes : `X-XFlow-Timestamp` (secondes unix) et `X-XFlow-Signature: sha256=<hex>`,
  avec `hex = HMAC_SHA256(secret, "<timestamp>." + corps_brut)`. Fenêtre : 300 s, rejeu refusé.
- Toute erreur d'authentification renvoie le même `404`. Corps limité à 1 Mio (413), 120 requêtes/min/IP (429).
- Les workflows webhook existants sans secret sont désactivés (404) tant qu'aucun secret n'est généré.
- Le champ `trigger.webhook_secret` est obsolète et ignoré.
- Option : `XFLOW_WEBHOOK_KEK` (passphrase) chiffre les secrets au repos (Fernet).
- Les déclenchements internes (IPC `run`/`trigger`, `POST /run/{name}` authentifié, événements, cron) ne sont pas concernés.

## Isolation des plugins

XFlow passe par le système IPC de XCore pour appeler les autres plugins.
Le middleware d'authentification et les permissions XCore s'appliquent normalement.

## Bonnes pratiques

- Ne stockez pas de secrets dans les payloads de workflow — utilisez les variables d'environnement XCore.
- Utilisez les tags pour segmenter les workflows par équipe.
- Activez l'audit log pour tracer toutes les exécutions sensibles.
- En production, utilisez Redis avec TLS pour la queue de messages.
