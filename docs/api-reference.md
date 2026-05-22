# Référence API — XFlow V2

Base URL : `http://localhost:8000/xflow`

Toutes les routes retournent un objet JSON avec le schéma XCore standard :
```json
{"status": "success"|"error", "data": {...}, "message": "..."}
```

---

## Workflows

### GET /flows
Liste tous les workflows déployés.

**Réponse :**
```json
{
  "status": "success",
  "data": {
    "workflows": [
      {"name": "onboarding", "version": "1.0.0", "trigger": "event", "tags": []}
    ]
  }
}
```

### POST /flows
Déploie un nouveau workflow (ou met à jour si le nom existe déjà).

**Body :** définition complète du workflow (voir workflow-dsl.md)

### GET /flows/{name}
Détail complet d'un workflow.

### DELETE /flows/{name}
Supprime un workflow et annule ses jobs schedulés.

### GET /flows/{name}/graph
Exporte le workflow sous forme de graph (nodes + edges) pour visualisation.

```json
{
  "nodes": [{"id": "step1", "type": "action", "label": "Envoyer mail"}],
  "edges": [{"source": "step1", "target": "step2", "label": "success"}]
}
```

---

## Exécutions

### POST /run/{name}
Déclenche un workflow immédiatement.

**Body :** payload de trigger (optionnel)

**Réponse :**
```json
{"run_id": "uuid", "status": "pending"}
```

### GET /executions
Liste les dernières exécutions.

**Params :** `workflow_name` (optionnel), `limit` (défaut: 50)

### GET /executions/{run_id}
Détail complet d'un run (statut, steps, contexte, erreurs).

### POST /executions/{run_id}/cancel
Annule un run en cours.

---

## Registry

### GET /registry
Liste toutes les actions IPC découvertes dans les plugins XCore.

```json
{
  "actions": [
    {
      "plugin": "mail",
      "action": "send",
      "qualified_name": "mail.send",
      "description": "Envoie un email",
      "input_schema": {...}
    }
  ]
}
```

---

## Webhooks

### POST /webhook/{name}
Déclenche un workflow via webhook externe.

Accepte n'importe quel body JSON — transmis comme payload de trigger.

Seuls les workflows avec `trigger.type = "webhook"` ou `"manual"` peuvent être déclenchés.

---

## IPC Actions (via XCore IPC)

| Action | Description |
|---|---|
| `xflow.register` | Déployer un workflow |
| `xflow.unregister` | Supprimer un workflow |
| `xflow.run` | Déclencher un workflow |
| `xflow.get_run` | État d'un run |
| `xflow.list_runs` | Lister les runs |
| `xflow.cancel_run` | Annuler un run |
| `xflow.list_workflows` | Lister les workflows |
| `xflow.registry` | Catalogue des actions |
| `xflow.ai_generate` | Générer via IA |
