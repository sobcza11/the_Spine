from pathlib import Path

import pandas as pd
import plotly.graph_objects as go


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "serving" / "c_flow" / "c_flow_serving_v4.parquet"
    out_path = repo_root / "data" / "serving" / "c_flow" / "c_flow_validation_graph_v1.html"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing input file: {in_path}")

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])
    latest = df.sort_values("date").iloc[-1]

    components = {
        "FX": latest["fx_pressure"],
        "Rates": latest["rates_pressure"],
        "Macro": latest["macro_pressure"],
        "Equity": latest["equity_pressure"],
        "COT Crypto": latest["cot_crypto_proxy_pressure"],
        "BTC COT": latest["btc_cot_pressure"],
        "Commodity": latest["commodity_pressure"],
        "Credit": latest["credit_pressure"],
        "Flows": latest["fund_flow_pressure"],
    }

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=list(components.keys()),
            y=list(components.values()),
            text=[f"{float(v):.3f}" for v in components.values()],
            textposition="outside",
        )
    )

    fig.update_layout(
        title=(
            f"C_FLOW Validation Graph v2 | "
            f"{latest['c_flow_state']} | "
            f"Score={float(latest['c_flow_score']):.3f}"
        ),
        template="plotly_dark",
        height=650,
        width=1100,
        xaxis_title="Component",
        yaxis_title="Pressure / Contribution",
        showlegend=False,
    )

    fig.add_hline(y=0, line_width=1, line_dash="dash")

    fig.write_html(out_path, include_plotlyjs="cdn", full_html=True)

    print("OK | C_FLOW validation graph v2")
    print(f"input: {in_path}")
    print(f"output: {out_path}")
    print("")
    print("components:")
    for k, v in components.items():
        print(f"{k}: {float(v):.3f}")


if __name__ == "__main__":
    main()