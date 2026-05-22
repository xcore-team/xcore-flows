# IPC Registry — Auto-découverte des plugins

## Comment fonctionne la découverte

Au démarrage, XFlow appelle `kernel.list_plugins` pour obtenir la liste des plugins actifs.
Pour chaque plugin, il tente d'appeler `{plugin}.xflow_integration`.

Si le plugin répond avec un catalogue d'actions, celles-ci sont indexées dans le registry.

## Consultation du registry

```bash
curl http://localhost:8000/xflow/registry
```

Ou via IPC depuis un autre plugin :
```python
result = await self.call_plugin("xflow.registry", {})
actions = result["data"]["actions"]
```

## Format du catalogue

```json
{
  "ipc_actions": [
    {
      "name": "create",
      "description": "Crée un utilisateur",
      "input_schema": {
        "email": {"type": "string", "required": true},
        "name": {"type": "string", "required": true},
        "role": {"type": "string", "default": "user"}
      },
      "output_schema": {
        "user_id": {"type": "string"},
        "created_at": {"type": "datetime"}
      }
    }
  ]
}
```

## Refresh du registry

Le registry est mis à jour :
- Au démarrage de XFlow
- Lors du chargement d'un nouveau plugin (événement `plugin.loaded`)
- Via l'appel `GET /xflow/registry` (force un re-scan si TTL expiré)
