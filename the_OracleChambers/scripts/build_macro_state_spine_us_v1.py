import argparse
from pathlib import Path

import duckdb


def build_macro_state_spine_us_v1(duckdb_path: str, output_path: str) -> None:
    """
    macro_state_spine_us.v1

    Fuses:
      - macro.econ_leaf (econ_score)
      - macro.inflation_leaf (inflation_score)
      - macro.wti_pressure_leaf (wti_pressure_score)
      - fedspeak.combined_policy_leaf (daily policy risks)
      - equity.vinv_signal (vinv_score)
      - technicals.technical_leaf (overall_technical_regime)

    Produces a daily macro-state panel with:
      - raw component scores
      - simple composite indices
      - coarse macro_state_label (pro_value / neutral / anti_value)
    """

    db_file = Path(duckdb_path)
    if not db_file.is_file():
        raise FileNotFoundError(f"DuckDB file not found: {db_file}")

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(database=str(db_file), read_only=False)

    sql = f"""
    COPY (
        WITH fedspeak_daily AS (
            SELECT
                CAST(event_date AS DATE) AS date,
                AVG(policy_bias)      AS avg_policy_bias,
                AVG(inflation_risk)   AS avg_inflation_risk,
                AVG(growth_risk)      AS avg_growth_risk
            FROM fedspeak.combined_policy_leaf
            WHERE event_date IS NOT NULL
            GROUP BY CAST(event_date AS DATE)
        ),
        base AS (
            SELECT
                m.date,
                m.econ_score,
                i.inflation_score,
                w.wti_pressure_score,
                f.avg_policy_bias,
                f.avg_inflation_risk,
                f.avg_growth_risk,
                v.vinv_score,
                t.overall_technical_regime
            FROM macro.econ_leaf AS m
            LEFT JOIN macro.inflation_leaf AS i
              ON m.date = i.date
            LEFT JOIN macro.wti_pressure_leaf AS w
              ON m.date = w.date
            LEFT JOIN fedspeak_daily AS f
              ON m.date = f.date
            LEFT JOIN equity.vinv_signal AS v
              ON m.date = v.date
            LEFT JOIN technicals.technical_leaf AS t
              ON m.date = t.date
        ),
        scored AS (
            SELECT
                date,
                econ_score,
                inflation_score,
                wti_pressure_score,
                avg_policy_bias,
                avg_inflation_risk,
                avg_growth_risk,
                vinv_score,
                overall_technical_regime,

                -- Simple macro heat index: growth minus inflation and WTI pressure
                (COALESCE(econ_score, 0.0)
                 - COALESCE(inflation_score, 0.0)
                 - COALESCE(wti_pressure_score, 0.0)) AS macro_heat,

                -- Policy stance index: policy + inflation_risk - growth_risk
                (COALESCE(avg_policy_bias, 0.0)
                 + COALESCE(avg_inflation_risk, 0.0)
                 - COALESCE(avg_growth_risk, 0.0)) AS policy_stance,

                -- Simple risk/technical index (placeholder)
                COALESCE(overall_technical_regime, 0.0) AS technical_index

            FROM base
        ),
        labeled AS (
            SELECT
                *,
                CASE
                    WHEN vinv_score IS NULL THEN 'unknown'
                    WHEN vinv_score >=  0.25 THEN 'pro_value'
                    WHEN vinv_score <= -0.25 THEN 'anti_value'
                    ELSE 'neutral'
                END AS macro_state_label
            FROM scored
        )
        SELECT *
        FROM labeled
        WHERE date IS NOT NULL
        ORDER BY date
    )
    TO '{out_file.as_posix()}' (FORMAT 'parquet');
    """

    print(f"[macro_state_spine_us.v1] Using DuckDB: {duckdb_path}")
    print(f"[macro_state_spine_us.v1] Writing output Parquet: {output_path}")
    con.execute(sql)
    con.close()
    print("[macro_state_spine_us.v1] Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build macro_state_spine_us.v1 fusion panel."
    )
    parser.add_argument(
        "--duckdb",
        required=True,
        help="Path to DuckDB file (e.g. data/warehouse/spine.duckdb).",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output Parquet (e.g. data/processed/fusion/macro_state_spine_us.parquet).",
    )
    args = parser.parse_args()

    build_macro_state_spine_us_v1(args.duckdb, args.output)


if __name__ == "__main__":
    main()

