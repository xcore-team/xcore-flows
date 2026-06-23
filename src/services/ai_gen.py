"""
Générateur de workflows assisté par IA — XFlow V2.
Transforme un prompt en langage naturel en définition de workflow JSON valide.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional, Protocol

import yaml

logger = logging.getLogger("xflow.ai_gen")


class _PluginLike(Protocol):
    async def call_plugin(self, plugin_action: str, payload: Dict[str, Any]) -> Dict[str, Any]: ...


class AIWorkflowGenerator:
    def __init__(self, plugin: _PluginLike, discovery_service: Any) -> None:
        self.plugin = plugin
        self.discovery = discovery_service

    async def generate_from_prompt(self, prompt: str) -> Dict[str, Any]:
        """
        Génère une définition de workflow JSON depuis un prompt utilisateur.
        Injecte le catalogue d'actions disponibles dans le contexte IA.
        """
        available_actions = self.discovery.list_available_actions()
        actions_context = "\n".join([
            f"- {a['qualified_name']}: {a['description']} | inputs: {json.dumps(a['input_schema'])}"
            for a in available_actions
        ]) or "Aucune action découverte pour l'instant."

        system_prompt = (
            "Tu es l'architecte XFlow V2, expert en automatisation de workflows pour la plateforme XCore.\n"
            "Ta mission : générer un workflow JSON valide et complet à partir d'une description en langage naturel.\n\n"
            f"Actions IPC disponibles :\n{actions_context}\n\n"
            "Format de réponse attendu — JSON uniquement, sans commentaire, sans backtick :\n"
            "{\n"
            '  "name": "nom_snake_case",\n'
            '  "version": "1.0.0",\n'
            '  "description": "description courte",\n'
            '  "trigger": { "type": "manual"|"event"|"schedule"|"webhook", ... },\n'
            '  "steps": [\n'
            '    { "id": "step1", "type": "action", "plugin": "...", "action": "...", "payload": {}, "on_success": "step2" },\n'
            '    ...\n'
            '  ]\n'
            "}\n\n"
            "Règles :\n"
            "- Chaque step doit avoir un 'id' unique.\n"
            "- Le dernier step doit avoir 'on_success': null ou ne pas avoir de 'on_success'.\n"
            "- Utilise {{ trigger.X }} pour les données d'entrée.\n"
            "- Utilise {{ steps.ID.result.X }} pour les résultats des steps précédents.\n"
            "- Réponds UNIQUEMENT avec le JSON valide."
        )

        try:
            res = await self.plugin.call_plugin("ai.generate", {
                "prompt": prompt,
                "system_prompt": system_prompt,
                "format": "json",
            })

            if not isinstance(res, dict) or res.get("status") != "success":
                raise ValueError(f"Erreur AI plugin : {res.get('message', str(res))}")

            data = res.get("data", {})
            workflow = data.get("workflow") or data.get("result") or data.get("text")

            if isinstance(workflow, str):
                # Nettoyage des backticks markdown éventuels
                workflow = workflow.strip()
                if workflow.startswith("```"):
                    workflow = "\n".join(workflow.split("\n")[1:])
                if workflow.endswith("```"):
                    workflow = "\n".join(workflow.split("\n")[:-1])
                workflow = json.loads(workflow.strip())

            if not isinstance(workflow, dict):
                raise ValueError("La réponse IA n'est pas un objet JSON valide.")

            logger.info("Workflow généré par IA : '%s'", workflow.get("name", "?"))
            return workflow

        except json.JSONDecodeError as exc:
            logger.error("JSON invalide dans la réponse IA : %s", exc)
            raise ValueError(f"La réponse IA n'est pas du JSON valide : {exc}") from exc
        except Exception as exc:
            logger.error("Échec de la génération IA : %s", exc)
            raise

    async def generate_yaml_from_prompt(self, prompt: str) -> str:
        """Retourne le workflow sous forme YAML."""
        data = await self.generate_from_prompt(prompt)
        return yaml.dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)

    async def optimize_workflow(self, flow_id: str) -> Dict[str, Any]:
        """
        Analyse un workflow existant et propose des optimisations.
        (Stub — nécessite le plugin ai dans XCore)
        """
        raise NotImplementedError("optimize_workflow sera disponible dans XFlow V2.1")

    async def debug_run(self, run_id: str) -> Dict[str, Any]:
        """
        Analyse un run échoué et propose des corrections.
        (Stub — nécessite le plugin ai dans XCore)
        """
        raise NotImplementedError("debug_run sera disponible dans XFlow V2.1")
