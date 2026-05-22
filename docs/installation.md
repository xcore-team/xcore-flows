# Installation de XFlow V2

## Prérequis

- XCore >= 2.0
- Python >= 3.12
- PostgreSQL ou SQLite (via SQLAlchemy)
- Redis (optionnel mais recommandé en production)

## Étapes d'installation

### 1. Copier le plugin

```bash
cp -r plugins/xflow /chemin/vers/xcore/plugins/
```

### 2. Installer les dépendances Python

```bash
pip install -r plugins/xflow/requirements.txt
```

Si vous utilisez un environnement virtuel XCore :

```bash
cd /chemin/vers/xcore
source .venv/bin/activate
pip install -r plugins/xflow/requirements.txt
```

### 3. Configuration (optionnel)

XFlow se configure automatiquement à partir des services XCore disponibles.
Aucun fichier `.env` supplémentaire n'est requis.

Si vous souhaitez forcer certains comportements, vous pouvez passer des variables
d'environnement XCore standard :

| Variable | Défaut | Description |
|---|---|---|
| `XFLOW_QUEUE_KEY` | `xflow:queue:tasks` | Clé Redis de la queue |
| `XFLOW_WORKER_SLEEP` | `0.5` | Pause (s) si queue vide |
| `XFLOW_CRASH_RECOVERY` | `true` | Reprendre les runs crashés |

### 4. Démarrer XCore

```bash
python main.py
```

XFlow sera chargé automatiquement par le PluginManager XCore.

### Vérification

```bash
# Doit retourner la liste des actions IPC découvertes
curl http://localhost:8000/xflow/registry

# Doit retourner une liste vide (aucun workflow encore)
curl http://localhost:8000/xflow/flows
```

## Installation en production

### Avec Redis (recommandé)

XFlow détecte automatiquement Redis via le service `cache` de XCore.
Si `cache` est configuré avec Redis, XFlow l'utilise comme queue de messages.

### Sans Redis (mode local)

XFlow bascule automatiquement sur une queue mémoire locale.
Adapté pour les environnements de développement ou les déploiements simples.

> ⚠️ En mode local, les runs en cours sont perdus si le processus redémarre.

## Désinstallation

```bash
# Arrêter XCore
# Supprimer le répertoire
rm -rf /chemin/vers/xcore/plugins/xflow

# (Optionnel) Supprimer les tables DB
# DROP TABLE xflow_flows, xflow_versions, xflow_runs, xflow_steps,
#            xflow_schedules, xflow_dead_jobs, xflow_audit_logs;
```
