from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from spine.jobs.geoscen.eis.exceptions import ResponseValidationError, UpstreamResponseError


def load_hud_payload(response_text: str) -> Mapping[str, Any]:
    try:
        import json
        payload = json.loads(response_text)
    except Exception as exc:
        text = response_text.strip()
        if text.lower().startswith("<!doctype html") or text.lower().startswith("<html"):
            raise UpstreamResponseError("HUD response was HTML, not JSON.", provider="hud") from exc
        raise UpstreamResponseError("Malformed HUD JSON response.", provider="hud") from exc
    if not isinstance(payload, Mapping):
        raise ResponseValidationError("HUD payload must be an object.", provider="hud")
    status = str(payload.get("status") or payload.get("code") or "").lower()
    if status in {"error", "failed", "failure"} or "error" in payload:
        message = str(payload.get("message") or payload.get("error") or "HUD API error")[:240]
        raise UpstreamResponseError("HUD API returned an error payload.", provider="hud", context={"message": message})
    return payload


def extract_records(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    data = payload.get("data", payload.get("results", payload.get("result")))
    if isinstance(data, Mapping):
        if isinstance(data.get("results"), list):
            data = data["results"]
        elif isinstance(data.get("basicdata"), list):
            data = data["basicdata"]
        elif isinstance(data.get("data"), list):
            data = data["data"]
        else:
            data = [data]
    if data is None and any(key in payload for key in ("states", "counties", "metros")):
        data = payload.get("states") or payload.get("counties") or payload.get("metros")
    if data is None:
        return []
    if isinstance(data, list):
        return [item for item in data if isinstance(item, Mapping)]
    raise ResponseValidationError("HUD data payload malformed.", provider="hud")


def parse_money(value: object) -> tuple[int | float | None, str, str | None]:
    if value is None:
        return None, "unavailable", None
    raw = str(value).strip()
    if raw in {"", "-", "--", "NA", "N/A", "null"}:
        return None, "unavailable", raw
    try:
        number = Decimal(raw.replace("$", "").replace(",", ""))
    except InvalidOperation:
        return None, "malformed", raw
    if number == number.to_integral_value():
        return int(number), "available", raw
    return float(number), "available", raw


def first(row: Mapping[str, Any], *keys: str) -> Any:
    lowered = {str(key).lower(): value for key, value in row.items()}
    for key in keys:
        if key in row:
            return row[key]
        if key.lower() in lowered:
            return lowered[key.lower()]
    return None


def bounded(value: object, limit: int = 240) -> str | None:
    if value is None:
        return None
    return str(value)[:limit]
