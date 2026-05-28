from dataclasses import dataclass


@dataclass(frozen=True)
class RuntimeConfig:
    system_name: str = "IsoVector"
    runtime_mode: str = "offline_review_runtime"

    deterministic_runtime: bool = True
    replayable_runtime: bool = True
    governance_enabled: bool = True

    ai_narration_allowed: bool = False
    doctrine_narration_allowed: bool = False

    state_processing_priority: bool = True
    contradiction_detection_enabled: bool = True
    cross_asset_transmission_enabled: bool = True

    max_zt_lines: int = 3
    max_rbl_lines: int = 3
    max_systemic_lines: int = 3

    reserved_space_states = {
        "contained": "-",
        "moderate": "Expanded",
        "high": "Systemic Escalation",
    }


CONFIG = RuntimeConfig()
