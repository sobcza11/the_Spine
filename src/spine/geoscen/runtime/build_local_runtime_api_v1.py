from pathlib import Path
from flask import Flask, jsonify
import json


app = Flask(__name__)


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@app.route("/runtime/status")
def runtime_status():

    repo_root = Path.cwd()

    status = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "visibility"
        / "recursive_status_runtime_v1.json"
    )

    return jsonify(status)


@app.route("/runtime/dashboard")
def runtime_dashboard():

    repo_root = Path.cwd()

    dashboard = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "dashboard"
        / "offline_executive_dashboard_v1.json"
    )

    return jsonify(dashboard)


if __name__ == "__main__":

    print("GeoScen Local Runtime API active")
    print("http://127.0.0.1:5000/runtime/status")
    print("http://127.0.0.1:5000/runtime/dashboard")

    app.run(
        host="127.0.0.1",
        port=5000,
        debug=False,
    )
