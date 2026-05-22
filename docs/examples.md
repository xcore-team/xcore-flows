# Exemples de workflows XFlow V2

Les fichiers YAML complets sont disponibles dans `/examples/`.

## 1. Onboarding client (`examples/onboarding_client.yaml`)

Déclenché par l'événement `client.created`, ce workflow :
- Crée l'espace de travail
- Envoie un email de bienvenue (avec retry)
- Active la période d'essai billing
- Envoie une notification push

## 2. Relance factures (`examples/relance_facture.yaml`)

Cron quotidien à 9h :
- Récupère les factures en retard
- Vérifie qu'il y en a au moins une (Condition)
- Itère sur chaque facture (Foreach)
- Envoie un email de relance

## 3. Pipeline IA support (`examples/ai_pipeline.yaml`)

Déclenché par `support.ticket_created` :
- Classifie le ticket (AI step)
- Route selon la catégorie (Switch)
- Répond automatiquement pour billing/technique
- Escalade en parallèle pour les urgences (Parallel)
- Envoie la réponse automatique

## 4. Workflow de paiement échoué

```yaml
name: paiement_echoue
trigger:
  type: event
  event_name: payment.failed

steps:
  - id: verifier_tentatives
    type: action
    plugin: billing
    action: get_retry_count
    payload:
      payment_id: "{{ trigger.payment_id }}"
    on_success: router_tentatives

  - id: router_tentatives
    type: switch
    expression: "{{ steps.verifier_tentatives.result.count }}"
    cases:
      "1": relancer_dans_24h
      "2": relancer_dans_48h
    default: suspendre_compte

  - id: relancer_dans_24h
    type: action
    plugin: scheduler
    action: schedule_once
    payload:
      delay_hours: 24
      workflow: retry_payment
      data: "{{ trigger }}"

  - id: suspendre_compte
    type: parallel
    branches:
      - [suspendre_acces]
      - [notifier_client]
      - [notifier_finance]
    wait_all: true
```

## 5. Génération de rapport hebdomadaire

```yaml
name: rapport_hebdomadaire
trigger:
  type: schedule
  cron: "0 8 * * 1"

steps:
  - id: collecter_stats
    type: parallel
    branches:
      - [stats_ventes]
      - [stats_support]
      - [stats_users]
    wait_all: true
    on_success: generer_rapport

  - id: stats_ventes
    type: action
    plugin: billing
    action: weekly_stats

  - id: stats_support
    type: action
    plugin: support
    action: weekly_stats

  - id: stats_users
    type: action
    plugin: users
    action: weekly_stats

  - id: generer_rapport
    type: ai
    service: summarize
    prompt: |
      Génère un résumé exécutif hebdomadaire basé sur ces données :
      Ventes: {{ steps.stats_ventes.result }}
      Support: {{ steps.stats_support.result }}
      Utilisateurs: {{ steps.stats_users.result }}
    on_success: envoyer_rapport

  - id: envoyer_rapport
    type: action
    plugin: mail
    action: send
    payload:
      to: ["ceo@company.com", "cto@company.com"]
      subject: "Rapport hebdomadaire — {{ trigger._triggered_by }}"
      body: "{{ steps.generer_rapport.result.summary }}"
```
