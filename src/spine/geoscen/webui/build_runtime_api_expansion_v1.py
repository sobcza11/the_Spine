from pathlib import Path
from flask import Flask, jsonify
import json


app = Flask(__name__)


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


BASE = (
    Path.cwd()
    / "data"
    / "geoscen"
)


@app.route("/api/runtime")
def runtime():

    return jsonify(
        load_json(
            BASE
            / "visibility"
            / "recursive_status_runtime_v1.json"
        )
    )


@app.route("/api/dashboard")
def dashboard():

    return jsonify(
        load_json(
            BASE
            / "dashboard"
            / "offline_executive_dashboard_v1.json"
        )
    )


@app.route("/api/timeline")
def timeline():

    return jsonify(
        load_json(
            BASE
            / "visibility"
            / "timeline"
            / "recursive_timeline_engine_summary_v1.json"
        )
    )


@app.route("/api/regime")
def regime():

    return jsonify(
        load_json(
            BASE
            / "intelligence"
            / "recursive_regime_classification_summary_v1.json"
        )
    )


if __name__ == "__main__":

    print("GeoScen Expanded Runtime API active")
    print("http://127.0.0.1:8090/api/runtime")

    app.run(
        host="127.0.0.1",
        port=8090,
        debug=False,
    )
