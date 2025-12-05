from pathlib import Path
import pandas as pd


def main():
    this_file = Path(__file__).resolve()

    # ROOT_SPINE = .../the_Spine
    ROOT_SPINE = this_file.parents[3]
    # ROOT_OC = .../the_Spine/the_OracleChambers
    ROOT_OC = this_file.parents[2]

    vinv_panel_path = (
        ROOT_SPINE
        / "data"
        / "vinv"
        / "vinv_2_0"
        / "phase2_outputs"
        / "vinv_monthly_panel_phase2.parquet"
    )
    macro_path = ROOT_OC / "data" / "macro" / "macro_signals_monthly.parquet"

    out_path = (
        ROOT_SPINE
        / "data"
        / "vinv"
        / "vinv_2_0"
        / "phase2_outputs"
        / "vinv_monthly_panel_v2_with_macro.parquet"
    )

    print(f"[VinV+Macro] Reading VinV panel:  {vinv_panel_path}")
    df_vinv = pd.read_parquet(vinv_panel_path)

    print(f"[VinV+Macro] Reading macro panel: {macro_path}")
    df_macro = pd.read_parquet(macro_path)

    # Ensure dates are aligned as datetime
    df_vinv["date"] = pd.to_datetime(df_vinv["date"])
    df_macro["date"] = pd.to_datetime(df_macro["date"])

    # Keep macro columns except 'date'
    macro_cols = [c for c in df_macro.columns if c != "date"]
    df_macro_small = df_macro[["date"] + macro_cols].drop_duplicates(subset=["date"])

    print(f"[VinV+Macro] VinV shape before join:   {df_vinv.shape}")
    print(f"[VinV+Macro] Macro shape before join:  {df_macro_small.shape}")

    # m:1 join — many VinV rows per month, one macro row per month
    df_out = df_vinv.merge(
        df_macro_small,
        on="date",
        how="left",
        validate="m:1",
    )

    print(f"[VinV+Macro] Output shape after join: {df_out.shape}")
    print(df_out.head())

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(out_path, index=False)
    print(f"[VinV+Macro] Wrote combined panel → {out_path}")


if __name__ == "__main__":
    main()
