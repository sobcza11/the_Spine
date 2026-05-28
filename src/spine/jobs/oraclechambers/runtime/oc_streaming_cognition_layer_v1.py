from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass(frozen=True)
class StreamingChannel:
    channel_id: str
    source_plane: str
    target_plane: str
    status: str
    description: str


def build_streaming_cognition_layer_v1() -> dict[str, Any]:
    channels = [
        StreamingChannel(
            channel_id="stream_core_to_exec",
            source_plane="oc-core-runtime-local",
            target_plane="oc-executive-dashboard-local",
            status="defined",
            description="Streams hydration-state updates into the executive dashboard plane.",
        ),
        StreamingChannel(
            channel_id="stream_contradiction_to_exec",
            source_plane="oc-contradiction-matrix-local",
            target_plane="oc-executive-dashboard-local",
            status="defined",
            description="Streams contradiction topology changes into executive attention routing.",
        ),
        StreamingChannel(
            channel_id="stream_visual_to_exec",
            source_plane="oc-macro-heatmap-local",
            target_plane="oc-executive-dashboard-local",
            status="defined",
            description="Streams visual intelligence changes into the command surface.",
        ),
        StreamingChannel(
            channel_id="stream_nlp_to_exec",
            source_plane="oc-narrative-drift-local",
            target_plane="oc-executive-dashboard-local",
            status="governed",
            description="Streams NLP cognition only as advisory context, never runtime truth.",
        ),
    ]

    return {
        "artifact": "oc_streaming_cognition_layer_v1",
        "layer": "OracleChambers Streaming Cognition Layer",
        "run_ts": datetime.now(timezone.utc).isoformat(),
        "streaming_layer_ready": True,
        "websocket_enabled": False,
        "online_transition_allowed": False,
        "channels": [channel.__dict__ for channel in channels],
    }


if __name__ == "__main__":
    output = build_streaming_cognition_layer_v1()

    for key, value in output.items():
        print(f"{key}: {value}")
        