from __future__ import annotations

HUD_HOST = "https://www.huduser.gov"
HUD_API_ROOT = f"{HUD_HOST}/hudapi/public"

PATH_BY_OPERATION = {
    "list_states": "/fmr/listStates",
    "list_counties": "/fmr/listCounties/{state_id}",
    "list_metro_areas": "/fmr/listMetroAreas",
    "fmr_data": "/fmr/data/{entity_id}",
    "fmr_state_data": "/fmr/statedata/{state_code}",
    "income_limits_data": "/il/data/{entity_id}",
    "income_limits_state_data": "/il/statedata/{state_code}",
}

DATASET_BY_OPERATION = {
    "list_states": "geography_lookup",
    "list_counties": "geography_lookup",
    "list_metro_areas": "geography_lookup",
    "fmr_data": "fmr",
    "fmr_state_data": "fmr",
    "income_limits_data": "income_limits",
    "income_limits_state_data": "income_limits",
}

TRANSFORMATION_BY_OPERATION = {
    "list_states": "hud-state-list-v1",
    "list_counties": "hud-county-list-v1",
    "list_metro_areas": "hud-metro-list-v1",
    "fmr_data": "hud-fmr-entity-v1",
    "fmr_state_data": "hud-fmr-state-v1",
    "income_limits_data": "hud-income-limits-entity-v1",
    "income_limits_state_data": "hud-income-limits-state-v1",
}


def endpoint_for(operation: str, *, state_id: str | None = None, state_code: str | None = None, entity_id: str | None = None) -> str:
    path = PATH_BY_OPERATION[operation]
    if "{state_id}" in path:
        path = path.format(state_id=state_id)
    if "{state_code}" in path:
        path = path.format(state_code=state_code)
    if "{entity_id}" in path:
        path = path.format(entity_id=entity_id)
    return f"{HUD_API_ROOT}{path}"
