from pathlib import Path
import pandas as pd


INPUT_PATH = Path("data/raw/ism_pmi_transp.xlsx")
OUT_DIR = Path("data/macro/serving")

OUT_DIR.mkdir(parents=True, exist_ok=True)


SHEET_PAIRS = [
    ("manu", "m_pmi", "m_no"),
    ("serv", "serv_pmi", "serv_no"),
]


def clean_industry(col: str, suffix: str) -> str:
    return str(col).replace(suffix, "").strip()


def melt_sheet(path: Path, sheet: str, suffix: str, value_name: str) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name=sheet)
    df = df.rename(columns={"dates": "date"})
    df["date"] = pd.to_datetime(df["date"])

    value_cols = [c for c in df.columns if c != "date"]

    out = df.melt(
        id_vars=["date"],
        value_vars=value_cols,
        var_name="industry",
        value_name=value_name,
    )

    out["industry"] = out["industry"].apply(lambda x: clean_industry(x, suffix))
    out[value_name] = pd.to_numeric(out[value_name], errors="coerce")

    return out


def main() -> None:
    if not INPUT_PATH.exists():
        raise FileNotFoundError(f"Missing file: {INPUT_PATH}")

    frames = []

    for pmi_type, pmi_sheet, no_sheet in SHEET_PAIRS:
        pmi = melt_sheet(INPUT_PATH, pmi_sheet, "_pmi", "pmi_Idx")
        no = melt_sheet(INPUT_PATH, no_sheet, "_no", "no_Idx")

        merged = pmi.merge(
            no,
            on=["date", "industry"],
            how="outer",
        )

        merged["pmi_type"] = pmi_type
        frames.append(merged)

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["pmi_type", "industry", "date"]).reset_index(drop=True)

    df["pmi_3M_Δ"] = df.groupby(["pmi_type", "industry"])["pmi_Idx"].diff(3)
    df["pmi_6M_Δ"] = df.groupby(["pmi_type", "industry"])["pmi_Idx"].diff(6)
    df["no_3M_Δ"] = df.groupby(["pmi_type", "industry"])["no_Idx"].diff(3)
    df["no_6M_Δ"] = df.groupby(["pmi_type", "industry"])["no_Idx"].diff(6)

    df["Sig"] = (
        0.45 * df["pmi_Idx"] +
        0.25 * df["no_Idx"] +
        0.15 * df["pmi_3M_Δ"] +
        0.15 * df["no_3M_Δ"]
    )

    df["Wt"] = 1.0

    df = df.dropna(subset=["date", "industry"]).reset_index(drop=True)

    panel_out = OUT_DIR / "ism_pmi_industry_panel.parquet"
    serving_out = OUT_DIR / "industry_panel_serving.json"
    latest_out = OUT_DIR / "ism_pmi_latest.json"

    df.to_parquet(panel_out, index=False)

    serving = df.copy()
    serving["date"] = serving["date"].dt.strftime("%Y-%m-%d")
    serving.to_json(serving_out, orient="records", indent=2)

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")
    latest.to_json(latest_out, orient="records", indent=2)

    print("PASS")
    print(f"Panel: {panel_out}")
    print(f"Serving JSON: {serving_out}")
    print(f"Latest JSON: {latest_out}")
    print("Latest date:", latest_date.strftime("%Y-%m-%d"))
    print("Rows:", len(df))


if __name__ == "__main__":
    main()

    