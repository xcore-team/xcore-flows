# Sécurité — XFlow V2

## Modèle de confiance

XFlow est un plugin **Trusted** dans XCore. Il dispose des permissions suivantes :
- `xflow.*` — toutes les actions XFlow
- `db.*` — accès complet à la base de données
- `cache.*` — accès complet au cache
- `scheduler.*` — gestion des jobs schedulés
- `plugins.*` — appel de n'importe quel plugin

## Authentification des webhooks entrants

Les webhooks sont actuellement non authentifiés par défaut.

Pour sécuriser un endpoint webhook :

1. Définissez un `webhook_secret` dans le trigger de votre workflow :
```json
{
  "trigger": {
    "type": "webhook",
    "webhook_secret": "mon_secret_sha256"
  }
}
```

2. XFlow vérifiera la signature HMAC-SHA256 du body via le header `X-XFlow-Signature`.

## Isolation des plugins

XFlow passe par le système IPC de XCore pour appeler les autres plugins.
Le middleware d'authentification et les permissions XCore s'appliquent normalement.

## Bonnes pratiques

- Ne stockez pas de secrets dans les payloads de workflow — utilisez les variables d'environnement XCore.
- Utilisez les tags pour segmenter les workflows par équipe.
- Activez l'audit log pour tracer toutes les exécutions sensibles.
- En production, utilisez Redis avec TLS pour la queue de messages.
