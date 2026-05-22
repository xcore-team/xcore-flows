# Démarrage rapide avec XFlow V2

## Concepts clés

**Workflow** — Séquence d'actions automatisées, définie en JSON ou YAML.

**Step** — Un noeud du workflow (Action, Condition, Foreach, etc.).

**Run** — Une exécution concrète d'un workflow, avec un identifiant unique.

**Trigger** — Ce qui déclenche un workflow (événement, cron, webhook, API).

**Registry** — Catalogue de toutes les actions IPC disponibles dans XCore.

---

## Votre premier workflow

### 1. Déployer un workflow

```bash
curl -X POST http://localhost:8000/xflow/flows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "ping_test",
    "trigger": {"type": "manual"},
    "steps": [
      {
        "id": "log_step",
        "type": "action",
        "plugin": "logger",
        "action": "info",
        "payload": {"message": "XFlow fonctionne !"}
      }
    ]
  }'
```

### 2. Déclencher le workflow

```bash
curl -X POST http://localhost:8000/xflow/run/ping_test \
  -H "Content-Type: application/json" \
  -d '{"custom_data": "hello"}'
```

Réponse :
```json
{
  "status": "success",
  "data": {
    "run_id": "3f8a2b1c-...",
    "status": "pending"
  }
}
```

### 3. Suivre l'exécution

```bash
curl http://localhost:8000/xflow/executions/3f8a2b1c-...
```

---

## Utiliser les variables

XFlow utilise la syntaxe `{{ variable }}` pour interpoler les données :

```json
{
  "id": "envoyer_mail",
  "type": "action",
  "plugin": "mail",
  "action": "send",
  "payload": {
    "to": "{{ trigger.email }}",
    "subject": "Bienvenue {{ trigger.name }} !",
    "body": "Votre ID est {{ trigger.user_id }}"
  }
}
```

Variables disponibles :

| Chemin | Description |
|---|---|
| `{{ trigger.X }}` | Données du trigger / payload d'entrée |
| `{{ steps.ID.result.X }}` | Résultat d'un step précédent |
| `{{ loop_item.X }}` | Élément courant dans un Foreach |
| `{{ run.id }}` | ID du run en cours |
| `{{ run.workflow }}` | Nom du workflow |

---

## Générer un workflow par IA

```bash
curl -X POST http://localhost:8000/xflow/registry \
  -H "Content-Type: application/json" \
  -d '{}'

# Puis via IPC :
curl -X POST http://localhost:8000/xflow/flows \
  -H "Content-Type: application/json" \
  -d '{
    "action": "ai_generate",
    "payload": {
      "prompt": "Crée un workflow qui envoie un email de bienvenue et crée un espace de travail quand un client signe"
    }
  }'
```

---

## Voir le catalogue des actions disponibles

```bash
curl http://localhost:8000/xflow/registry
```

Résultat :
```json
{
  "status": "success",
  "data": {
    "actions": [
      {
        "plugin": "mail",
        "action": "send",
        "qualified_name": "mail.send",
        "description": "Envoie un email",
        "input_schema": {"to": "string", "subject": "string", "body": "string"}
      },
      ...
    ]
  }
}
```
