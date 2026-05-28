from pathlib import Path
import html

import pandas as pd


PANEL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>{title}</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body {{
      margin: 0;
      background: #05070b;
      color: #e6edf3;
      font-family: Arial, Helvetica, sans-serif;
    }}
    .page {{ padding: 24px; }}
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
        <div class="toolbar-title">{toolbar}</div>
        <div class="toolbar-date">{date_label}</div>
      </div>

      <div class="grid">
        <div class="box">
          <div class="box-title">Z_t | {zt_name}</div>
          <div class="metric-main">{zt_value}</div>
          <div class="metric-sub">{zt_note}</div>
        </div>

        <div class="box">
          <div class="box-title">RESERVED •|• Graph</div>
          <div class="placeholder">{graph_note}</div>
        </div>
      </div>

      <div class="bottom-grid">
        <div class="box">
          <div class="box-title">RBL | READ BETWEEN THE LINES (OC • {rbl_name})</div>
          <div class="rbl">{rbl_oc}</div>
        </div>

        <div class="box">
          <div class="box-title">Final Metric</div>
          <div class="metric-main">{final_metric}</div>
          <div class="metric-sub">{final_note}</div>
        </div>
      </div>

      <div class="footer">{footer}</div>
    </div>
  </div>
</body>
</html>
"""


def safe_text(value):
    return html.escape(str(value)).replace("\\n", "<br>").replace("\n", "<br>")


def latest_row(path):
    df = pd.read_parquet(path).copy()
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").iloc[-1]


def write_panel(path, **kwargs):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(PANEL_HTML.format(**kwargs), encoding="utf-8")


def main():
    repo_root = Path.cwd()
    base = repo_root / "data" / "serving"

    macro = latest_row(base / "geoscen" / "geoscen_serving_v2.parquet")
    fx = latest_row(base / "fx" / "fx_serving_v2.parquet")
    rates = latest_row(base / "rates" / "rates_serving_v2.parquet")
    equities = latest_row(base / "equities" / "equities_serving_v2.parquet")

    write_panel(
        base / "geoscen" / "geoscen_panel_v1.html",
        title="MACRO | OC",
        toolbar="IsoVector.io | OC | MACRO",
        zt_name="MACRO",
        rbl_name="MACRO",
        date_label=f"Latest: {macro['date'].strftime('%Y-%m-%d')}",
        zt_value=f"{float(macro['tone_direction']):.3f}",
        zt_note=f"GeoScen context | Dominance Mean: {float(macro['dominance_mean']):.3f} | Confidence: {float(macro['regime_confidence']):.3f}",
        graph_note="Macro / GeoScen validation graph reserved",
        rbl_oc=safe_text(macro["rbl_oc"]),
        final_metric=f"{float(macro['regime_confidence']):.3f}",
        final_note=str(macro["regime_label"]),
        footer="Source: GeoScen serving v2 | Local only | IsoVector.io standard",
    )

    write_panel(
        base / "equities" / "equities_market_indexes_panel_v1.html",
        title="EQUITY | MARKET INDX | OC",
        toolbar="IsoVector.io | OC | EQUITY | MARKET INDX",
        zt_name="EQUITY | MARKET INDX",
        rbl_name="EQUITY • MARKET INDX",
        date_label=f"Latest: {equities['date'].strftime('%Y-%m-%d')}",
        zt_value="--",
        zt_note="Market index signal layer pending final computation",
        graph_note="Market index validation graph reserved",
        rbl_oc="Equity Market Index: Broad-market signal layer is not yet complete. No directional index view is supported. Overall: excluded from system state pending signal completion.",
        final_metric="--",
        final_note="Reserved Equity Market Index Metric",
        footer="Source: the_Spine | Equity Market Index | Offline panel v1 | IsoVector.io standard",
    )

    write_panel(
        base / "equities" / "equities_industry_sectors_panel_v1.html",
        title="EQUITY | INDUSTRY/SECTORS | OC",
        toolbar="IsoVector.io | OC | EQUITY | INDUSTRY/SECTORS",
        zt_name="EQUITY | INDUSTRY/SECTORS",
        rbl_name="EQUITY • INDUSTRY/SECTORS",
        date_label=f"Latest: {equities['date'].strftime('%Y-%m-%d')}",
        zt_value="--",
        zt_note="Industry / sector signal layer pending final computation",
        graph_note="Industry / sector validation graph reserved",
        rbl_oc=safe_text(equities["rbl_oc"]),
        final_metric="--",
        final_note="Reserved Equity Industry/Sector Metric",
        footer="Source: the_Spine | Equity Industry/Sectors | Offline panel v1 | IsoVector.io standard",
    )

    write_panel(
        base / "equities" / "equities_panel_v1.html",
        title="EQUITY | INDUSTRY/SECTORS | OC",
        toolbar="IsoVector.io | OC | EQUITY | INDUSTRY/SECTORS",
        zt_name="EQUITY | INDUSTRY/SECTORS",
        rbl_name="EQUITY • INDUSTRY/SECTORS",
        date_label=f"Latest: {equities['date'].strftime('%Y-%m-%d')}",
        zt_value="--",
        zt_note="Industry / sector signal layer pending final computation",
        graph_note="Industry / sector validation graph reserved",
        rbl_oc=safe_text(equities["rbl_oc"]),
        final_metric="--",
        final_note="Reserved Equity Industry/Sector Metric",
        footer="Source: the_Spine | Equity Industry/Sectors | Offline panel v1 | IsoVector.io standard",
    )

    write_panel(
        base / "c_flow" / "c_flow_panel_v1.html",
        title="C_FLOW | OC",
        toolbar="IsoVector.io | OC | C_FLOW",
        zt_name="C_FLOW",
        rbl_name="C_FLOW",
        date_label="Latest: --",
        zt_value="--",
        zt_note="Capital flow layer pending final computation",
        graph_note="C_FLOW validation graph reserved",
        rbl_oc="C_FLOW: Capital-flow signal layer is reserved. No directional flow view is supported yet. Overall: excluded from system state pending data completion.",
        final_metric="--",
        final_note="Reserved C_FLOW Metric",
        footer="Source: the_Spine | C_FLOW | Offline panel v1 | IsoVector.io standard",
    )

    write_panel(
        base / "fx" / "fx_panel_v1.html",
        title="FX | OC",
        toolbar="IsoVector.io | OC | FX",
        zt_name="FX",
        rbl_name="FX",
        date_label=f"Latest: {fx['date'].strftime('%Y-%m-%d')}",
        zt_value=f"{float(fx['tone_direction']):.3f}",
        zt_note="FX condition proxy | local normalized panel",
        graph_note="FX validation graph reserved",
        rbl_oc=safe_text(fx["rbl_oc"]),
        final_metric="RESERVED",
        final_note="Reserved FX Metric",
        footer="Source: FX serving v2 | Local only | IsoVector.io standard",
    )

    write_panel(
        base / "rates" / "rates_panel_v1.html",
        title="RATES | OC",
        toolbar="IsoVector.io | OC | RATES",
        zt_name="RATES",
        rbl_name="RATES",
        date_label=f"Latest: {rates['date'].strftime('%Y-%m-%d')}",
        zt_value=f"{float(rates['dominance_mean']):.3f}",
        zt_note="Rates condition proxy | local normalized panel",
        graph_note="Rates validation graph reserved",
        rbl_oc=safe_text(rates["rbl_oc"]),
        final_metric="--",
        final_note="Reserved Rates Composite",
        footer="Source: Rates serving v2 | Local only | IsoVector.io standard",
    )

    print("OK | OC panels updated to v3")
    print(base / "geoscen" / "geoscen_panel_v1.html")
    print(base / "equities" / "equities_market_indexes_panel_v1.html")
    print(base / "equities" / "equities_industry_sectors_panel_v1.html")
    print(base / "c_flow" / "c_flow_panel_v1.html")
    print(base / "fx" / "fx_panel_v1.html")
    print(base / "rates" / "rates_panel_v1.html")


if __name__ == "__main__":
    main()