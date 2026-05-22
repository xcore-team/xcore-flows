# Dépannage — XFlow V2

## Le plugin ne se charge pas

**Symptôme :** XFlow n'apparaît pas dans les plugins chargés.

**Vérifications :**
1. Le répertoire `plugins/xflow` est bien présent.
2. Le `plugin.yaml` est valide.
3. Les dépendances sont installées : `pip install -r requirements.txt`
4. Vérifier les logs XCore au démarrage.

## Un workflow reste en statut "pending"

**Cause probable :** le worker loop est arrêté ou la queue est bloquée.

**Solution :**
```bash
# Vérifier l'état du plugin
curl http://localhost:8000/xflow/registry

# Redémarrer XCore si nécessaire
# Le crash recovery reprendra automatiquement les runs pending
```

## Erreur "Workflow introuvable"

Vérifiez que le workflow a bien été déployé :
```bash
curl http://localhost:8000/xflow/flows
```

## Un step échoue avec "Plugin non disponible"

Le plugin cible n'est pas chargé dans XCore. Vérifiez :
1. Que le plugin est installé dans `/plugins/`
2. Que ses dépendances sont installées
3. Les logs du plugin au démarrage

## Les crons ne se déclenchent pas

Le service `scheduler` de XCore doit être disponible. Si XCore n'inclut pas APScheduler,
les triggers `schedule` ne fonctionneront pas — un warning est loggué au démarrage.

## Erreur de connexion Redis

XFlow bascule automatiquement sur la queue locale. Pour forcer Redis, assurez-vous
que le service `cache` de XCore est configuré avec une URL Redis valide.

## Logs utiles

```bash
# Niveau DEBUG pour XFlow
export LOG_LEVEL=DEBUG
python main.py 2>&1 | grep xflow
```
