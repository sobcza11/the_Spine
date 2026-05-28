from pathlib import Path
from flask import Flask, render_template_string
import json


app = Flask(__name__)


HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>

<title>GeoScen Runtime</title>

<style>

body {
    background-color: #0f1117;
    color: #e6edf3;
    font-family: Arial;
    margin: 40px;
}

.card {
    background-color: #161b22;
    padding: 20px;
    margin-bottom: 20px;
    border-radius: 10px;
}

h1 {
    color: #58a6ff;
}

.metric {
    font-size: 20px;
    margin-bottom: 10px;
}

.good {
    color: #3fb950;
}

.watch {
    color: #d29922;
}

.bad {
    color: #f85149;
}

</style>

</head>

<body>

<h1>GeoScen Institutional Runtime</h1>

<div class="card">

<h2>Executive Runtime</h2>

<div class="metric">
Runtime Health:
<span class="good">
{{runtime_health}}
</span>
</div>

<div class="metric">
Systemic Risk:
<span class="watch">
{{systemic_risk}}
</span>
</div>

<div class="metric">
Global Recursive Mode:
<span class="watch">
{{global_mode}}
</span>
</div>

<div class="metric">
Highest Risk Branch:
<span class="bad">
{{risk_branch}}
</span>
</div>

</div>

<div class="card">

<h2>Executive Read</h2>

<p>
{{executive_read}}
</p>

</div>

<div class="card">

<h2>Recursive Timeline</h2>

<div class="metric">
Transition State:
{{transition_state}}
</div>

<div class="metric">
Recursive Direction:
{{recursive_direction}}
</div>

<div class="metric">
Regime Shift Events:
{{regime_shift_events}}
</div>

</div>

<div class="card">

<h2>Governance</h2>

<div class="metric">
Governance State:
{{governance_state}}
</div>

<div class="metric">
Recursion Mode:
{{recursion_mode}}
</div>

<div class="metric">
Governance Action:
{{governance_action}}
</div>

</div>

</body>
</html>
"""


def load_json(path):

    if not path.exists():
        return {}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


@app.route("/")
def dashboard():

    repo_root = Path.cwd()

    dashboard = load_json(
        repo_root
        / "data"
        / "geoscen"
        / "dashboard"
        / "offline_executive_dashboard_v1.json"
    )

    return render_template_string(

        HTML_TEMPLATE,

        runtime_health=dashboard[
            "executive_header"
        ][
            "runtime_health"
        ],

        systemic_risk=dashboard[
            "executive_header"
        ][
            "systemic_risk_level"
        ],

        global_mode=dashboard[
            "executive_header"
        ][
            "global_recursive_mode"
        ],

        risk_branch=dashboard[
            "scenario_conditions"
        ][
            "highest_risk_branch"
        ],

        executive_read=dashboard[
            "executive_header"
        ][
            "executive_read"
        ],

        transition_state=dashboard[
            "timeline_conditions"
        ][
            "latest_transition_state"
        ],

        recursive_direction=dashboard[
            "timeline_conditions"
        ][
            "latest_recursive_direction"
        ],

        regime_shift_events=dashboard[
            "timeline_conditions"
        ][
            "regime_shift_events"
        ],

        governance_state=dashboard[
            "governance_conditions"
        ][
            "governance_state"
        ],

        recursion_mode=dashboard[
            "governance_conditions"
        ][
            "recursion_mode"
        ],

        governance_action=dashboard[
            "governance_conditions"
        ][
            "governance_action"
        ],
    )


if __name__ == "__main__":

    print("GeoScen Offline Web UI active")
    print("http://127.0.0.1:8080")

    app.run(
        host="127.0.0.1",
        port=8080,
        debug=False,
    )
