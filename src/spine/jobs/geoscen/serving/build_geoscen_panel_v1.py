from pathlib import Path
import html

import pandas as pd


def fmt_num(x, digits=3):
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return "NA"


def main():
    repo_root = Path.cwd()

    in_path = repo_root / "data" / "serving" / "geoscen" / "geoscen_serving_v1.parquet"
    out_path = repo_root / "data" / "serving" / "geoscen" / "geoscen_panel_v1.html"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing input file: {in_path}")

    df = pd.read_parquet(in_path).copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    latest = df.iloc[-1]

    date = latest["date"].strftime("%Y-%m-%d")
    regime_label = html.escape(str(latest["regime_label"]))
    regime_confidence = fmt_num(latest["regime_confidence"])
    dominance_mean = fmt_num(latest["dominance_mean"])
    signal_strength = fmt_num(latest["signal_strength"], 6)
    tone_direction = fmt_num(latest["tone_direction"])
    rbl_report = html.escape(str(latest["rbl_report_with_regime"])).replace("\\n", "<br>").replace("\n", "<br>")

    html_doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>GeoScen Panel v1</title>
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
      box-shadow: 0 0 24px rgba(0,0,0,0.35);
      overflow: hidden;
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
    <div class="panel geoscen-panel">

      <div class="toolbar">
        <div class="toolbar-title">GeoScen | OC | RBL, Regime</div>
        <div class="toolbar-date">Latest: {date}</div>
      </div>

      <div class="grid">
        <div class="box">
          <div class="box-title">Zₜ / GeoScen Context</div>
          <div class="metric-main">{tone_direction}</div>
          <div class="metric-sub">Tone Direction</div>
          <div class="metric-sub">Dominance Mean: {dominance_mean}</div>
          <div class="metric-sub">Signal Strength: {signal_strength}</div>
        </div>

        <div class="box">
          <div class="box-title">RESERVED •|• Graph</div>
          <div class="placeholder">Graph validation placeholder</div>
        </div>
      </div>

      <div class="bottom-grid">
        <div class="box">
          <div class="box-title">RBL •|• Reading Between the Lines with OC</div>
          <div class="rbl">{rbl_report}</div>
        </div>

        <div class="box">
          <div class="box-title">Final GeoScen Metric</div>
          <div class="metric-main">{regime_confidence}</div>
          <div class="metric-sub">Regime Confidence</div>
          <div class="metric-sub">Regime:</div>
          <div class="metric-sub"><strong>{regime_label}</strong></div>
        </div>
      </div>

      <div class="footer">
        Source: GeoScen serving v1 | Context only | Confidence-aware | No direct trading recommendation
      </div>

    </div>
  </div>
</body>
</html>
"""

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(html_doc, encoding="utf-8")

    print("OK | GeoScen panel v1")
    print(f"output: {out_path}")


if __name__ == "__main__":
    main()