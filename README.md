# XFlow V2 — Moteur de Workflows Enterprise pour XCore

<p align="center">
  <img src="docs/assets/xflow-banner.svg" width="600" alt="XFlow V2">
</p>

> **XFlow** est le moteur d'automatisation central de la plateforme XCore.
> Il orchestre n'importe quel plugin installé pour créer des workflows intelligents,
> event-driven, résistants aux pannes et extensibles à l'infini.

---

## Pourquoi XFlow ?

Dans XCore, chaque plugin expose des **actions IPC** :
`mail.send`, `users.create`, `billing.invoice`, `crm.update`, `ai.generate`…

XFlow transforme ces actions en **noeuds réutilisables** et vous permet de créer
des automatisations complexes **sans écrire une seule ligne de code**.

---

## Fonctionnalités

| Catégorie | Fonctionnalité |
|---|---|
| **Triggers** | Event Bus, Cron, Webhook, IPC, Manuel |
| **Node types** | Action, Condition, Switch, Foreach, Parallel, Wait, Transform, Template, AI |
| **Fiabilité** | Retry exponentiel, Timeout, Dead-letter queue, Crash recovery |
| **Persistance** | SQLAlchemy async, Redis queue, cache TTL |
| **Observabilité** | Logs structurés, Audit trail, Métriques par run |
| **IA** | Génération de workflow par prompt naturel |
| **API** | REST complète, Webhooks sortants, Graph export |

---

## Installation

### 1. Copier le plugin dans XCore

```bash
cp -r plugins/xflow /chemin/vers/xcore/plugins/
```

### 2. Installer les dépendances

```bash
cd /chemin/vers/xcore
pip install -r plugins/xflow/requirements.txt
```

### 3. Démarrer XCore

```bash
python main.py
# XFlow se charge automatiquement via le PluginManager XCore
```

### 4. Vérifier le chargement

```bash
curl http://localhost:8000/xflow/registry
# → Liste des actions IPC découvertes dans tous les plugins
```

---

## Quick Start — Premier workflow en 2 minutes

### Via l'API REST

```bash
curl -X POST http://localhost:8000/xflow/flows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "hello_world",
    "trigger": {"type": "manual"},
    "steps": [
      {
        "id": "send_mail",
        "type": "action",
        "plugin": "mail",
        "action": "send",
        "payload": {
          "to": "admin@example.com",
          "subject": "Hello depuis XFlow !",
          "body": "Mon premier workflow fonctionne."
        }
      }
    ]
  }'
```

### Déclencher le workflow

```bash
curl -X POST http://localhost:8000/xflow/run/hello_world \
  -H "Content-Type: application/json" \
  -d '{}'
# → {"status": "success", "data": {"run_id": "abc-123", "status": "pending"}}
```

### Consulter l'état

```bash
curl http://localhost:8000/xflow/executions/abc-123
```

---

## Exemples de workflows

### Onboarding client

```yaml
name: onboarding_client
trigger:
  type: event
  event_name: client.created

steps:
  - id: create_workspace
    type: action
    plugin: users
    action: create_workspace
    payload:
      client_id: "{{ trigger.client_id }}"
    on_success: send_welcome_mail

  - id: send_welcome_mail
    type: action
    plugin: mail
    action: send
    payload:
      to: "{{ trigger.email }}"
      template: welcome
    on_success: create_trial

  - id: create_trial
    type: action
    plugin: billing
    action: create_trial
    payload:
      client_id: "{{ trigger.client_id }}"
      days: 14
```

### Relance factures (cron)

```yaml
name: relance_facture
trigger:
  type: schedule
  cron: "0 9 * * *"

steps:
  - id: get_overdue
    type: action
    plugin: billing
    action: overdue_list
    on_success: iterate_invoices

  - id: iterate_invoices
    type: foreach
    items: "{{ steps.get_overdue.result.invoices }}"
    steps: [send_reminder]

  - id: send_reminder
    type: action
    plugin: mail
    action: send
    payload:
      to: "{{ loop_item.client_email }}"
      template: invoice_reminder
```

---

## Routes API

| Méthode | Route | Description |
|---|---|---|
| GET | `/xflow/flows` | Lister tous les workflows |
| POST | `/xflow/flows` | Créer / déployer un workflow |
| GET | `/xflow/flows/{name}` | Détail d'un workflow |
| DELETE | `/xflow/flows/{name}` | Supprimer un workflow |
| GET | `/xflow/flows/{name}/graph` | Export du graph (nodes/edges) |
| POST | `/xflow/run/{name}` | Déclencher un workflow |
| GET | `/xflow/executions` | Lister les runs |
| GET | `/xflow/executions/{run_id}` | Détail d'un run |
| POST | `/xflow/executions/{run_id}/cancel` | Annuler un run |
| GET | `/xflow/registry` | Catalogue des actions IPC |
| POST | `/xflow/webhook/{name}` | Trigger webhook |

---

## Architecture

```
XFlow V2
├── Plugin (main.py)         ← Point d'entrée, IPC + HTTP
├── WorkflowEngine           ← Exécution async des steps
├── WorkflowRegistryService  ← CRUD workflows + scheduler
├── WorkflowStore            ← Persistance SQLAlchemy + cache Redis
├── DiscoveryService         ← Scan automatique des plugins XCore
├── WorkflowScheduler        ← Intégration APScheduler de XCore
├── AIWorkflowGenerator      ← Génération de workflows par IA
└── LocalQueue               ← Fallback queue mémoire
```

**Flux d'exécution :**

```
Trigger (Event/Cron/API/Webhook)
    ↓
init_run() → DB [pending]
    ↓
lpush → Redis Queue (ou LocalQueue)
    ↓
Worker Loop → rpop
    ↓
execute_existing() → WorkflowEngine
    ↓
Step-by-step (Action / Condition / Foreach / ...)
    ↓
call_plugin(plugin.action, payload)
    ↓
DB [success/failed] + EventBus emit + Webhooks sortants
```

---

## Compatibilité XCore

| Version XCore | XFlow V2 |
|---|---|
| >= 2.0 | ✅ Supporté |
| < 2.0 | ❌ Non supporté |

**Permissions requises :** voir `plugin.yaml`

---

## Roadmap

- [ ] UI visuelle de conception de workflows (drag & drop)
- [ ] Versioning avancé des workflows (diff, rollback)
- [ ] Support des sous-workflows (call workflow from workflow)
- [ ] Métriques Prometheus natives
- [ ] Multi-tenant (isolation par workspace)
- [ ] Import/Export workflows en YAML
- [ ] Marketplace de templates

---

## Licence

Distribué sous licence MIT — voir `LICENSE`.
