# XFlow V2 — Guide d'intégration

**Plugin** : `xflow`  
**Version** : 2.0.0  
**Dépendances** : XCore SDK, SQLAlchemy (async), Redis (optionnel), APScheduler (optionnel)

---

## Table des matières

1. [Vue d'ensemble](#vue-densemble)
2. [Architecture](#architecture)
3. [Lifecycle du plugin](#lifecycle-du-plugin)
4. [Actions IPC](#actions-ipc)
5. [Routes HTTP](#routes-http)
6. [Définition d'un workflow](#définition-dun-workflow)
7. [Types de steps](#types-de-steps)
8. [Triggers](#triggers)
9. [Templates et variables](#templates-et-variables)
10. [Retry et gestion des erreurs](#retry-et-gestion-des-erreurs)
11. [Composite Nodes](#composite-nodes)
12. [Catalogue d'événements](#catalogue-dévénements)
13. [Webhooks sortants](#webhooks-sortants)
14. [Intégration avec un autre plugin](#intégration-avec-un-autre-plugin)
15. [Contrat XFlow d'un plugin tiers](#contrat-xflow-dun-plugin-tiers)

---

## Vue d'ensemble

XFlow V2 est le moteur d'orchestration central de la plateforme XCore. Il permet de définir, déployer et exécuter des workflows d'automatisation composés de steps séquentiels, conditionnels ou parallèles, déclenchés par des événements, des webhooks, des crons ou manuellement.

```
Trigger → WorkflowEngine → Steps (action / condition / parallel / ...) → Webhooks sortants
               ↑
          WorkerLoop (Redis ou LocalQueue)
```

---

## Architecture

```
src/
├── main.py                  # Plugin principal — actions IPC + routes HTTP
├── repositories/
│   ├── models.py            # Modèles SQLAlchemy (flows, runs, steps, schedules…)
│   └── workflow.py          # WorkflowStore — CRUD + cache Redis
├── runtime/
│   ├── engine.py            # WorkflowEngine — exécution stateless
│   ├── condition.py         # Évaluateur de templates {{ }} et conditions
│   └── retry.py             # Logique de retry avec backoff
├── schemas/
│   ├── workflow.py          # Pydantic — WorkflowDefinition, WorkflowRun, tous les steps
│   ├── composite.py         # Pydantic — CompositeNodeDefinition
│   └── events.py            # Pydantic — EventSchema, EventCatalogEntry
├── services/
│   ├── registry.py          # WorkflowRegistryService — CRUD + scheduler sync
│   ├── scheduler.py         # WorkflowScheduler — intégration APScheduler
│   ├── discovery.py         # DiscoveryService — scan des plugins XCore
│   ├── composites.py        # CompositeService — CRUD des composite nodes
│   ├── event_catalog.py     # EventCatalogService — catalogue d'événements
│   ├── ai_gen.py            # AIWorkflowGenerator — génération IA de workflows
│   └── webhooks.py          # dispatch_webhooks — notifications HTTP sortantes
└── workers/
    └── local_queue.py       # LocalQueue — fallback mémoire si pas de Redis
```

### Queue de tâches

XFlow utilise une queue nommée `xflow:queue:tasks`. Si Redis est disponible via le service `cache`, il est utilisé comme backend. Sinon, `LocalQueue` (FIFO en mémoire, non persistant) prend le relais automatiquement.

---

## Lifecycle du plugin

### `on_load`

1. Connexion à la DB (service `db`)
2. Initialisation de la queue (Redis ou `LocalQueue`)
3. Création des tables SQL si inexistantes
4. Initialisation du `WorkflowStore`, `WorkflowEngine`, `DiscoveryService`
5. Connexion au scheduler XCore (optionnel)
6. Sync des workflows SCHEDULE depuis la DB
7. Initialisation du `CompositeService` et `EventCatalogService`
8. Abonnement à tous les événements EventBus (`*`)
9. Scan initial des plugins actifs
10. Crash recovery : réenfilage des runs en statut `RUNNING`
11. Démarrage du worker loop

### `on_unload`

- Arrêt propre du worker loop
- Désactivation de tous les jobs schedulés

---

## Actions IPC

Toutes les actions acceptent et retournent du JSON. Les réponses suivent le contrat XCore : `{ "status": "success"|"error", "data": {...} }`.

### `register` / `deploy`

Enregistre ou met à jour un workflow.

```json
{
  "definition": { /* WorkflowDefinition */ }
}
```

**Réponse**
```json
{ "workflow_name": "mon_workflow", "message": "Workflow déployé avec succès." }
```

---

### `unregister`

Supprime un workflow et son job schedulé associé.

```json
{ "workflow_name": "mon_workflow" }
```

---

### `run` / `trigger`

Déclenche un workflow de façon asynchrone. Retourne immédiatement l'ID du run.

```json
{
  "workflow_name": "mon_workflow",
  "payload": { "user_id": "123", "action": "send_email" }
}
```

**Réponse**
```json
{ "run_id": "uuid", "status": "pending" }
```

---

### `get_run`

```json
{ "run_id": "uuid" }
```

---

### `executions` / `list_runs`

```json
{ "workflow_name": "mon_workflow", "limit": 50 }
```

---

### `cancel_run` / `pause`

```json
{ "run_id": "uuid" }
```

---

### `list_workflows`

Aucun payload requis. Retourne le catalogue de tous les workflows enregistrés.

---

### `registry`

Retourne le catalogue des actions IPC découvertes sur tous les plugins actifs.

---

### `ai_generate`

Génère une définition de workflow JSON depuis un prompt en langage naturel.

```json
{ "prompt": "Envoyer un email de bienvenue quand un utilisateur s'inscrit" }
```

**Réponse**
```json
{ "workflow": { /* WorkflowDefinition générée */ } }
```

---

### `composite.register`

```json
{
  "name": "send_and_notify",
  "steps": [ /* ... */ ],
  "inputs": [ { "name": "user_id", "required": true } ],
  "outputs": [ { "name": "result", "source_step": "step1", "source_field": "id" } ]
}
```

---

### `composite.list`

Aucun payload. Retourne tous les composites actifs.

---

### `composite.expand`

```json
{
  "composite_name": "send_and_notify",
  "instance_id": "node_42",
  "inputs": { "user_id": "{{ trigger.user_id }}" }
}
```

---

### `events.list`

Retourne le catalogue d'événements découverts.

---

## Routes HTTP

| Méthode | Chemin | Description |
|---------|--------|-------------|
| `GET` | `/flows` | Lister tous les workflows |
| `POST` | `/flows` | Créer / déployer un workflow |
| `GET` | `/flows/{name}` | Détail d'un workflow |
| `DELETE` | `/flows/{name}` | Supprimer un workflow |
| `GET` | `/flows/{name}/graph` | Export nodes/edges pour visualisation |
| `POST` | `/run/{name}` | Déclencher un workflow |
| `GET` | `/executions` | Lister les runs (`?workflow_name=&limit=`) |
| `GET` | `/executions/{run_id}` | Détail d'un run |
| `POST` | `/executions/{run_id}/cancel` | Annuler un run |
| `GET` | `/registry` | Catalogue des actions IPC |
| `POST` | `/webhook/{name}` | Endpoint webhook public |
| `GET` | `/composites` | Lister les composites |
| `POST` | `/composites` | Créer un composite |
| `GET` | `/composites/{name}` | Détail d'un composite |
| `DELETE` | `/composites/{name}` | Supprimer un composite |
| `POST` | `/composites/{name}/expand` | Étendre un composite |
| `GET` | `/events` | Catalogue d'événements |
| `POST` | `/events/refresh` | Rafraîchir le catalogue |

> **Webhook public** : l'endpoint `/webhook/{name}` accepte uniquement les workflows dont le trigger est `webhook` ou `manual`.

---

## Définition d'un workflow

```json
{
  "name": "notification_bienvenue",
  "version": "1.0.0",
  "description": "Envoie un email de bienvenue après inscription",
  "trigger": {
    "type": "event",
    "event_name": "user.registered"
  },
  "steps": [
    {
      "id": "envoyer_email",
      "type": "action",
      "plugin": "mailer",
      "action": "send",
      "payload": {
        "to": "{{ trigger.email }}",
        "subject": "Bienvenue {{ trigger.first_name }} !"
      },
      "on_success": "notifier_slack"
    },
    {
      "id": "notifier_slack",
      "type": "action",
      "plugin": "slack",
      "action": "post_message",
      "payload": {
        "channel": "#inscriptions",
        "text": "Nouvel utilisateur : {{ trigger.email }}"
      }
    }
  ],
  "timeout_seconds": 30
}
```

### Champs principaux

| Champ | Type | Requis | Description |
|-------|------|--------|-------------|
| `name` | string | ✅ | Identifiant unique snake_case |
| `version` | string | | Par défaut `1.0.0` |
| `description` | string | | Description courte |
| `trigger` | TriggerConfig | | Voir [Triggers](#triggers) |
| `steps` | AnyStep[] | ✅ | Au moins 1 step |
| `entry_step` | string | | ID du step de départ (défaut : premier step) |
| `webhooks` | WebhookNotification[] | | Notifications HTTP sortantes |
| `timeout_seconds` | float | | Timeout global du workflow |
| `tags` | string[] | | Tags libres |

---

## Types de steps

### `action` — Appel IPC vers un plugin

```json
{
  "id": "appel_api",
  "type": "action",
  "plugin": "mon_plugin",
  "action": "nom_action",
  "payload": { "cle": "{{ trigger.valeur }}" },
  "retry": { "max_attempts": 3, "delay_seconds": 5, "backoff": "exponential" },
  "on_success": "step_suivant",
  "on_failure": "step_erreur"
}
```

### `condition` — Branchement conditionnel

```json
{
  "id": "check_statut",
  "type": "condition",
  "condition": {
    "left": "{{ trigger.statut }}",
    "operator": "==",
    "right": "actif"
  },
  "if_true": "step_ok",
  "if_false": "step_ko"
}
```

Opérateurs disponibles : `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `contains`, `starts_with`, `ends_with`, `is_null`, `is_not_null`, `regex`

### `parallel` — Exécution parallèle

```json
{
  "id": "paralleliser",
  "type": "parallel",
  "branches": [["step_a1", "step_a2"], ["step_b1"]],
  "wait_all": true,
  "on_success": "rejoindre"
}
```

### `switch` — Aiguillage multi-cas

```json
{
  "id": "aiguillage",
  "type": "switch",
  "expression": "{{ trigger.type }}",
  "cases": {
    "email": "step_email",
    "sms": "step_sms"
  },
  "default": "step_fallback"
}
```

### `foreach` — Itération sur une liste

```json
{
  "id": "boucle",
  "type": "foreach",
  "items": "{{ trigger.utilisateurs }}",
  "steps": ["traiter_un"],
  "parallel": true,
  "max_parallel": 5,
  "on_success": "fin"
}
```

À l'intérieur des steps de la boucle, l'élément courant est accessible via `{{ loop_item }}`.

### `wait` — Délai ou attente d'événement

```json
{
  "id": "pause",
  "type": "wait",
  "delay_seconds": 60,
  "on_success": "step_suivant"
}
```

### `transform` — Transformation de données

```json
{
  "id": "transformer",
  "type": "transform",
  "query": "{{ steps.appel_api.result.data }}",
  "on_success": "step_suivant"
}
```

### `template` — Rendu de texte

```json
{
  "id": "rendre_message",
  "type": "template",
  "template": "Bonjour {{ trigger.prenom }}, votre commande {{ steps.creer_commande.result.id }} est confirmée.",
  "output_key": "message_final",
  "on_success": "envoyer"
}
```

Le rendu est accessible via `{{ steps.rendre_message.result.message_final }}`.

### `ai` — Appel au plugin IA

```json
{
  "id": "classifier",
  "type": "ai",
  "service": "classify",
  "prompt": "Classifie ce ticket : {{ trigger.texte }}",
  "on_success": "router"
}
```

Services disponibles : `summarize`, `classify`, `decide`, `extract`

---

## Triggers

### `manual`

Déclenché uniquement via IPC `run` ou HTTP `POST /run/{name}`.

```json
{ "type": "manual" }
```

### `event`

Déclenché automatiquement lorsqu'un événement est émis sur l'EventBus.

```json
{
  "type": "event",
  "event_name": "user.registered"
}
```

### `webhook`

Déclenché via `POST /webhook/{workflow_name}`. Le corps de la requête est passé comme `trigger`.

```json
{
  "type": "webhook",
  "webhook_secret": "mon_secret_optionnel"
}
```

### `schedule`

Déclenché par cron ou intervalle. Nécessite le service `scheduler` de XCore.

```json
{
  "type": "schedule",
  "cron": "0 9 * * 1-5"
}
```

```json
{
  "type": "schedule",
  "interval_seconds": 300
}
```

---

## Templates et variables

XFlow utilise la syntaxe `{{ chemin.vers.valeur }}` pour injecter des données dynamiques dans les payloads et expressions.

| Variable | Description |
|----------|-------------|
| `{{ trigger.X }}` | Données du payload déclencheur |
| `{{ steps.ID.result.X }}` | Résultat d'un step précédent |
| `{{ loop_item }}` | Élément courant dans un `foreach` |
| `{{ run.id }}` | ID du run en cours |
| `{{ run.workflow }}` | Nom du workflow |

**Résolution de chemin imbriqué**

```json
{ "user": "{{ trigger.user.profile.email }}" }
```

Si le chemin ne résout pas, la valeur retournée est `null` (pas d'erreur).

---

## Retry et gestion des erreurs

La configuration `retry` est disponible sur les steps `action`.

```json
{
  "retry": {
    "max_attempts": 5,
    "delay_seconds": 2.0,
    "backoff": "exponential",
    "max_delay_seconds": 120.0,
    "retry_on_codes": ["timeout", "rate_limit"]
  }
}
```

| Champ | Défaut | Description |
|-------|--------|-------------|
| `max_attempts` | `3` | Nombre maximum de tentatives (1–20) |
| `delay_seconds` | `5.0` | Délai initial entre tentatives |
| `backoff` | `exponential` | `constant`, `linear` ou `exponential` |
| `max_delay_seconds` | `300.0` | Plafond du délai calculé |
| `retry_on_codes` | `[]` | Codes d'erreur retryables (vide = tous) |

**Stratégies de backoff**

| Stratégie | Formule |
|-----------|---------|
| `constant` | `delay_seconds` |
| `linear` | `delay_seconds × attempt` |
| `exponential` | `delay_seconds × 2^(attempt-1)` |

Si toutes les tentatives échouent, une exception `RetryExhausted` est levée et le run passe en `failed`, sauf si `on_failure` est défini sur le step.

---

## Composite Nodes

Un composite node encapsule plusieurs steps en une unité réutilisable dans n'importe quel workflow.

### Définition

```json
{
  "name": "envoyer_et_logger",
  "description": "Envoie un message et l'enregistre",
  "steps": [
    {
      "id": "envoyer",
      "type": "action",
      "plugin": "mailer",
      "action": "send",
      "payload": { "to": "{{ inputs.email }}" },
      "on_success": "logger"
    },
    {
      "id": "logger",
      "type": "action",
      "plugin": "audit",
      "action": "log",
      "payload": { "message": "Email envoyé à {{ inputs.email }}" }
    }
  ],
  "inputs": [
    { "name": "email", "required": true, "description": "Adresse email du destinataire" }
  ],
  "outputs": [
    { "name": "log_id", "source_step": "logger", "source_field": "id" }
  ],
  "input_mappings": {
    "envoyer": { "email": "to" }
  }
}
```

### Utilisation dans un workflow

À l'exécution, le moteur appelle `composite.expand` pour substituer le node composite par ses steps internes, préfixés avec l'`instance_id` pour éviter les collisions d'ID.

---

## Catalogue d'événements

Le `EventCatalogService` découvre automatiquement les événements disponibles au démarrage via :

1. L'introspection des handlers enregistrés sur l'EventBus XCore
2. L'appel à `events.list` sur chaque plugin actif

**Consulter le catalogue**

```
GET /events
```

**Rafraîchir**

```
POST /events/refresh
```

---

## Webhooks sortants

Un workflow peut notifier des URLs externes à différentes étapes de son exécution.

```json
{
  "webhooks": [
    {
      "url": "https://mon-service.com/hooks/xflow",
      "method": "POST",
      "headers": { "Authorization": "Bearer mon_token" },
      "on_events": ["success", "failure"],
      "body_template": {
        "run_id": "{{ run.id }}",
        "workflow": "{{ run.workflow }}",
        "status": "{{ run.status }}"
      },
      "timeout_seconds": 10.0
    }
  ]
}
```

Événements disponibles : `start`, `success`, `failure`, `step_success`, `step_failure`

---

## Intégration avec un autre plugin

Pour appeler XFlow depuis un plugin XCore :

```python
# Déclencher un workflow
result = await self.call_plugin("xflow", "run", {
    "workflow_name": "notification_bienvenue",
    "payload": {
        "email": user.email,
        "first_name": user.first_name
    }
})

run_id = result["data"]["run_id"]

# Vérifier l'état du run
status = await self.call_plugin("xflow", "get_run", {
    "run_id": run_id
})
```

---

## Contrat XFlow d'un plugin tiers

Pour qu'un plugin soit **découvert et utilisable** dans les workflows XFlow, il doit exposer l'action IPC `xflow.integration` qui retourne son contrat :

```python
@action("xflow.integration")
async def xflow_integration(self, payload: dict) -> dict:
    return self._ok(
        display_name="Mon Plugin",
        description="Description courte du plugin",
        ipc_actions=[
            {
                "name": "envoyer_email",
                "description": "Envoie un email transactionnel",
                "input_schema": {
                    "to": "string",
                    "subject": "string",
                    "body": "string"
                }
            }
        ],
        events={
            "emits": ["email.sent", "email.failed"],
            "listens": []
        }
    )
```

Une fois enregistré, les actions du plugin apparaissent dans le catalogue `GET /registry` et peuvent être référencées dans les steps de type `action` :

```json
{
  "id": "step1",
  "type": "action",
  "plugin": "mon_plugin",
  "action": "envoyer_email",
  "payload": { "to": "{{ trigger.email }}" }
}
```

Le scan initial a lieu au démarrage de XFlow. Pour forcer un re-scan, redémarrer le plugin ou appeler `registry` via IPC.

---

## Tables SQL créées automatiquement

| Table | Description |
|-------|-------------|
| `xflow_flows` | Registre des workflows (nom, état actif) |
| `xflow_versions` | Versions des workflows (définition JSON) |
| `xflow_runs` | Historique des exécutions |
| `xflow_steps` | Résultats par step pour chaque run |
| `xflow_schedules` | Jobs cron/interval |
| `xflow_dead_jobs` | Dead-letter queue pour les jobs en échec |
| `xflow_audit_logs` | Journal d'audit par run |
| `xflow_composites` | Composite nodes enregistrés |

Les tables sont créées via `Base.metadata.create_all` au démarrage si elles n'existent pas encore.
