"""
Évaluateur de templates et de conditions pour le moteur de workflow XFlow V2.
"""
from __future__ import annotations

import re
from typing import Any, Dict

from ..schemas.workflow import ConditionConfig, ConditionOperator

_TEMPLATE_RE = re.compile(r"\{\{\s*([\w.]+)\s*\}\}")


def _resolve_path(path: str, data: Dict[str, Any]) -> Any:
    current: Any = data
    rest = path
    while rest:
        if isinstance(current, dict):
            # Les clés peuvent contenir des points (ex. id de step "auth_xauth.get_user_rk2f").
            # On matche la clé la plus longue qui préfixe le chemin restant sur une frontière de point.
            matched: str | None = None
            for key in current:
                if rest == key or rest.startswith(key + "."):
                    if matched is None or len(key) > len(matched):
                        matched = key
            if matched is None:
                return None
            current = current[matched]
            rest = rest[len(matched):].lstrip(".")
        else:
            part, _, rest = rest.partition(".")
            if hasattr(current, part):
                current = getattr(current, part)
            else:
                return None
    return current


def render_value(value: Any, context: Dict[str, Any]) -> Any:
    if isinstance(value, str):
        matches = _TEMPLATE_RE.findall(value)
        if not matches:
            return value
        full_match = _TEMPLATE_RE.fullmatch(value.strip())
        if full_match:
            return _resolve_path(full_match.group(1), context)

        def replacer(m: re.Match) -> str:
            resolved = _resolve_path(m.group(1), context)
            return str(resolved) if resolved is not None else m.group(0)

        return _TEMPLATE_RE.sub(replacer, value)

    if isinstance(value, dict):
        return {k: render_value(v, context) for k, v in value.items()}
    if isinstance(value, list):
        return [render_value(item, context) for item in value]
    return value


def render_payload(payload: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    return {k: render_value(v, context) for k, v in payload.items()}


def evaluate_condition(cfg: ConditionConfig, context: Dict[str, Any]) -> bool:
    left_raw = render_value(cfg.left, context)
    right_raw = render_value(cfg.right, context) if cfg.right is not None else None
    op = cfg.operator

    if op == ConditionOperator.IS_NULL:
        return left_raw is None
    if op == ConditionOperator.IS_NOT_NULL:
        return left_raw is not None

    left: Any = left_raw
    right: Any = right_raw

    try:
        left = float(str(left_raw))
        right = float(str(right_raw))
    except (TypeError, ValueError):
        pass

    if op == ConditionOperator.EQ:
        return left == right
    if op == ConditionOperator.NEQ:
        return left != right
    if op == ConditionOperator.GT:
        return left > right
    if op == ConditionOperator.GTE:
        return left >= right
    if op == ConditionOperator.LT:
        return left < right
    if op == ConditionOperator.LTE:
        return left <= right

    left_s = str(left_raw) if left_raw is not None else ""
    right_s = str(right_raw) if right_raw is not None else ""

    if op == ConditionOperator.CONTAINS:
        if isinstance(left_raw, (list, tuple)):
            return right_raw in left_raw
        return right_s in left_s
    if op == ConditionOperator.STARTS_WITH:
        return left_s.startswith(right_s)
    if op == ConditionOperator.ENDS_WITH:
        return left_s.endswith(right_s)
    if op == ConditionOperator.IN:
        if isinstance(right_raw, (list, tuple)):
            return left_raw in right_raw
        return left_s in right_s
    if op == ConditionOperator.NOT_IN:
        if isinstance(right_raw, (list, tuple)):
            return left_raw not in right_raw
        return left_s not in right_s
    if op == ConditionOperator.REGEX:
        return bool(re.search(right_s, left_s))

    raise ValueError(f"Opérateur inconnu : {op}")
