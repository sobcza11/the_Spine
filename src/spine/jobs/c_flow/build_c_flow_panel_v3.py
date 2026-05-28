from pathlib import Path
import html

import pandas as pd


def safe_text(value):
    return html.escape(str(value)).replace("\\n", "<br>").replace("\n", "<br>")


def fmt(value, digits=3):
    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return "--"


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "serving" / "c_flow" / "c_flow_serving_v3.parquet"
    out_path = repo_root / "data" / "serving" / "c_flow" / "c_flow_panel_v1.html"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing C_FLOW serving file: {in_path}")

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])

    latest = df.sort_values("date").iloc[-1]

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>C_FLOW | OC</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />

  <style>
    body {{
      margin: 0;
      background: #05070b;
      color: #e6edf3;
      font-family: Arial, Helvetica, sans-serif;
    }}

    .page {{
      padding: 24px;
    }}

    .panel {{
      border: 1px solid #263241;
      border-radius: 16px;
      background: #0b111a;
      overflow: hidden;
      box-shadow: 0 0 24px rgba(0,0,0,0.35);
    }}

    .toolbar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 14px 18px;
      border-bottom: 1px solid #263241;
      background: #0f1724;
    }}

    .toolbar-title {{
      font-size: 14px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #aeb9c7;
      font-weight: 700;
    }}

    .toolbar-date {{
      font-size: 13px;
      color: #7f8ea3;
    }}

    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
      padding: 16px;
    }}

    .bottom-grid {{
      display: grid;
      grid-template-columns: 3fr 1fr;
      gap: 14px;
      padding: 0 16px 16px 16px;
    }}

    .box {{
      border: 1px solid #263241;
      border-radius: 14px;
      background: #101826;
      padding: 16px;
      min-height: 150px;
    }}

    .box-title {{
      font-size: 13px;
      letter-spacing: 0.06em;
      text-transform: uppercase;
      color: #8fa3bd;
      margin-bottom: 12px;
    }}

    .metric-main {{
      font-size: 28px;
      font-weight: 700;
      margin-bottom: 8px;
    }}

    .metric-sub {{
      font-size: 13px;
      color: #aeb9c7;
      margin: 6px 0;
    }}

    .rbl {{
      font-size: 14px;
      line-height: 1.55;
      color: #d7dee8;
    }}

    .placeholder {{
      height: 110px;
      border: 1px dashed #33445a;
      border-radius: 12px;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #6f8095;
      font-size: 13px;
      text-align: center;
    }}

    .footer {{
      padding: 10px 18px 16px 18px;
      font-size: 12px;
      color: #66758a;
    }}
  </style>
</head>

<body>
  <div class="page">

    <div class="panel">

      <div class="toolbar">
        <div class="toolbar-title">
          IsoVector.io | OC | C_FLOW
        </div>

        <div class="toolbar-date">
          Latest: {latest["date"].strftime("%Y-%m-%d")}
        </div>
      </div>

      <div class="grid">

        <div class="box">
          <div class="box-title">Z_t | C_FLOW</div>

          <div class="metric-main">
            {fmt(latest["c_flow_score"])}
          </div>

          <div class="metric-sub">
            {safe_text(latest["c_flow_state"])}
          </div>

          <div class="metric-sub">
            Confidence: {fmt(latest["c_flow_confidence"])}
          </div>
        </div>

        <div class="box">
          <div class="box-title">RESERVED •|• Graph</div>

          <div class="placeholder">
            C_FLOW validation graph reserved<br>
            FX / Rates / Macro / Equity / COT / Commodity / Credit / Flows
          </div>
        </div>

      </div>

      <div class="bottom-grid">

        <div class="box">
          <div class="box-title">
            RBL | READ BETWEEN THE LINES (OC • C_FLOW)
          </div>

          <div class="rbl">
            {safe_text(latest["rbl_oc"])}
          </div>
        </div>

        <div class="box">

          <div class="box-title">
            Final Metric
          </div>

          <div class="metric-main">
            {fmt(latest["c_flow_score"])}
          </div>

          <div class="metric-sub">FX: {fmt(latest["fx_pressure"])}</div>
          <div class="metric-sub">Rates: {fmt(latest["rates_pressure"])}</div>
          <div class="metric-sub">Macro: {fmt(latest["macro_pressure"])}</div>
          <div class="metric-sub">Equity: {fmt(latest["equity_pressure"])}</div>
          <div class="metric-sub">COT: {fmt(latest["cot_pressure"])}</div>
          <div class="metric-sub">Commodity: {fmt(latest["commodity_pressure"])}</div>
          <div class="metric-sub">Credit: {fmt(latest["credit_pressure"])}</div>
          <div class="metric-sub">Flows: {fmt(latest["fund_flow_pressure"])}</div>

        </div>

      </div>

      <div class="footer">
        Source: C_FLOW serving v3 | Credit + COT + Commodity active | Direct institutional flows pending
      </div>

    </div>

  </div>
</body>
</html>
"""

    out_path.write_text(html_doc, encoding="utf-8")

    print("OK | C_FLOW panel v3")
    print(f"input: {in_path}")
    print(f"output: {out_path}")


if __name__ == "__main__":
    main()

    