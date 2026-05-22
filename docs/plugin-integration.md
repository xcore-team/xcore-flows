# Intégration de plugins tiers avec XFlow V2

## Auto-découverte

XFlow scanne automatiquement tous les plugins actifs dans XCore.

Pour qu'un plugin soit reconnu par XFlow, il doit exposer l'action IPC `xflow_integration`.

## Contrat d'intégration

```python
@action("xflow_integration")
async def ipc_xflow_integration(self, payload: dict) -> dict:
    return self._ok(
        ipc_actions=[
            {
                "name": "send",
                "description": "Envoie un email",
                "input_schema": {
                    "to": {"type": "string", "required": True},
                    "subject": {"type": "string", "required": True},
                    "body": {"type": "string", "required": False}
                },
                "output_schema": {
                    "message_id": {"type": "string"},
                    "sent_at": {"type": "datetime"}
                }
            },
            {
                "name": "send_bulk",
                "description": "Envoie un email en masse",
                "input_schema": {
                    "recipients": {"type": "array"},
                    "template": {"type": "string", "required": True}
                }
            }
        ]
    )
```

## Utilisation dans un workflow

Une fois découvert, `mail.send` devient un noeud :

```json
{
  "id": "envoyer_email",
  "type": "action",
  "plugin": "mail",
  "action": "send",
  "payload": {
    "to": "{{ trigger.email }}",
    "subject": "Notification automatique",
    "body": "{{ trigger.message }}"
  }
}
```

## Re-scan manuel

```bash
curl http://localhost:8000/xflow/registry
```

XFlow re-scanne aussi automatiquement :
- Au démarrage du plugin
- Toutes les heures (si configuré)
- Sur événement `plugin.loaded` de l'EventBus

## Plugins compatibles sans modification

Tout plugin XCore qui expose des actions IPC peut être utilisé dans XFlow,
même sans implémenter `xflow_integration`. Dans ce cas, XFlow utilisera
le nom d'action comme identifiant, sans métadonnées enrichies.

```json
{
  "id": "step",
  "type": "action",
  "plugin": "mon_plugin",
  "action": "mon_action",
  "payload": {}
}
```
