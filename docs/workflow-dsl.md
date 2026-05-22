# Référence DSL — Définition de Workflows XFlow V2

Un workflow XFlow est défini en JSON (ou YAML).

## Structure globale

```json
{
  "name": "nom_unique",
  "version": "1.0.0",
  "description": "Description optionnelle",
  "trigger": { ... },
  "steps": [ ... ],
  "entry_step": "id_du_premier_step",
  "timeout_seconds": 300,
  "tags": ["production", "billing"],
  "webhooks": [ ... ]
}
```

---

## Triggers

### Manuel
```json
{"type": "manual"}
```

### Event Bus
```json
{
  "type": "event",
  "event_name": "client.created"
}
```

### Cron (Schedule)
```json
{
  "type": "schedule",
  "cron": "0 9 * * 1-5"
}
```
Format cron : `minute heure jour mois jour_semaine`

### Interval
```json
{
  "type": "schedule",
  "interval_seconds": 3600
}
```

### Webhook
```json
{
  "type": "webhook"
}
```
Exposé sur : `POST /xflow/webhook/{workflow_name}`

---

## Types de Steps

### Action — Appel IPC vers un plugin

```json
{
  "id": "mon_step",
  "type": "action",
  "plugin": "mail",
  "action": "send",
  "payload": {
    "to": "{{ trigger.email }}"
  },
  "on_success": "step_suivant",
  "on_failure": "step_erreur",
  "timeout_seconds": 30,
  "retry": {
    "max_attempts": 3,
    "delay_seconds": 5,
    "backoff": "exponential",
    "max_delay_seconds": 60
  }
}
```

### Condition — Branchement if/else

```json
{
  "id": "verif_solde",
  "type": "condition",
  "condition": {
    "left": "{{ steps.get_account.result.balance }}",
    "operator": ">=",
    "right": "100"
  },
  "if_true": "step_ok",
  "if_false": "step_insuffisant"
}
```

Opérateurs disponibles : `==`, `!=`, `>`, `>=`, `<`, `<=`, `in`, `not_in`, `contains`, `starts_with`, `ends_with`, `is_null`, `is_not_null`, `regex`

### Switch — Routage multi-valeurs

```json
{
  "id": "router",
  "type": "switch",
  "expression": "{{ trigger.plan }}",
  "cases": {
    "starter": "step_starter",
    "pro": "step_pro",
    "enterprise": "step_enterprise"
  },
  "default": "step_starter"
}
```

### Parallel — Exécution en parallèle

```json
{
  "id": "notifications_paralleles",
  "type": "parallel",
  "branches": [
    ["envoyer_email"],
    ["envoyer_sms"],
    ["envoyer_push"]
  ],
  "wait_all": true,
  "on_success": "step_suivant"
}
```

### Foreach — Itération sur une liste

```json
{
  "id": "traiter_factures",
  "type": "foreach",
  "items": "{{ steps.get_invoices.result.list }}",
  "steps": ["relancer_facture"],
  "parallel": false,
  "max_parallel": 5,
  "on_success": "step_suivant"
}
```

Dans les steps du foreach, `{{ loop_item }}` pointe sur l'élément courant.

### Wait — Délai ou attente d'événement

```json
{
  "id": "attendre_confirmation",
  "type": "wait",
  "delay_seconds": 86400,
  "on_success": "step_suivant"
}
```

### Transform — Extraction de données

```json
{
  "id": "extraire_ids",
  "type": "transform",
  "query": "{{ steps.get_list.result.items }}",
  "on_success": "step_suivant"
}
```

### Template — Rendu Jinja2

```json
{
  "id": "generer_message",
  "type": "template",
  "template": "Bonjour {{ trigger.name }}, votre commande {{ trigger.order_id }} est confirmée.",
  "output_key": "message_final",
  "on_success": "step_suivant"
}
```

Résultat accessible via `{{ steps.generer_message.result.message_final }}`

### AI — Step IA natif

```json
{
  "id": "classifier",
  "type": "ai",
  "service": "classify",
  "prompt": "Classifie ce ticket : {{ trigger.subject }}",
  "on_success": "router",
  "on_failure": "fallback_humain"
}
```

Services disponibles : `summarize`, `classify`, `decide`, `extract`

---

## Retry

```json
"retry": {
  "max_attempts": 3,
  "delay_seconds": 5.0,
  "backoff": "exponential",
  "max_delay_seconds": 300.0,
  "retry_on_codes": ["rate_limit", "timeout"]
}
```

Stratégies backoff : `constant`, `linear`, `exponential`

---

## Webhooks sortants

```json
"webhooks": [
  {
    "url": "https://hooks.slack.com/services/...",
    "method": "POST",
    "on_events": ["success", "failure"],
    "body_template": {
      "text": "Workflow {{ run.workflow }} — {{ run.status }}"
    },
    "timeout_seconds": 10
  }
]
```

Événements disponibles : `start`, `success`, `failure`, `step_success`, `step_failure`
