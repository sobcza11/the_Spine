from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response

REPO_ROOT = Path.cwd()

HYDRATION_PATH = (
    REPO_ROOT
    / "data"
    / "serving"
    / "oraclechambers"
    / "oc_local_site_hydration_v1.json"
)

app = FastAPI(
    title="OracleChambers Local API v1",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "oraclechambers_local_api_v1",
    }


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/oc/hydration")
def get_oc_hydration() -> Response:
    if not HYDRATION_PATH.exists():
        return JSONResponse(
            status_code=404,
            content={
                "deployment_ready": False,
                "error": f"Missing hydration payload: {HYDRATION_PATH}",
            },
        )

    return Response(
        content=HYDRATION_PATH.read_text(encoding="utf-8"),
        media_type="application/json",
    )


@app.get("/oc/events/heartbeat")
def heartbeat() -> dict[str, Any]:
    return {
        "event": "heartbeat",
        "status": "active",
        "runtime_mode": "local_runtime",
        "ts": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/oc/events/refresh")
def refresh_event() -> dict[str, Any]:
    return {
        "event": "refresh_requested",
        "status": "accepted",
        "scope": "hydration_payload",
        "ts": datetime.now(timezone.utc).isoformat(),
    }

from datetime import datetime, timezone
from typing import Any


