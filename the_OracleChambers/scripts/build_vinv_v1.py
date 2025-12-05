import argparse
from pathlib import Path

import duckdb


def build_vinv_v1(duckdb_path: str, output_path: str) -> None:
    """
    VinV v1.0 — Structural Value Support Score

    Inputs (from DuckDB):
      - macro.econ_leaf (date, econ_score)
      - macro.inflation_leaf (date, inflation_score)
      - macro.wti_pressure_leaf (date, wti_pressure_score)
      - fedspeak.combined_policy_leaf (event_date, policy_bias, inflation_risk, growth_risk)

    Output (Parquet):
      - date
      - econ_score, inflation_score, wti_pressure_score
      - avg_policy_bias, avg_inflation_risk, avg_growth_risk
      - macro_value_support, policy_value_support
      - vinv_score in [-1, 1]
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
        joined AS (
            SELECT
                m.date,
                m.econ_score,
                i.inflation_score,
                w.wti_pressure_score,
                f.avg_policy_bias,
                f.avg_inflation_risk,
                f.avg_growth_risk
            FROM macro.econ_leaf AS m
            LEFT JOIN macro.inflation_leaf AS i
              ON m.date = i.date
            LEFT JOIN macro.wti_pressure_leaf AS w
              ON m.date = w.date
            LEFT JOIN fedspeak_daily AS f
              ON m.date = f.date
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

                -- Macro side: value prefers solid growth, calmer inflation, and non-explosive WTI
                (COALESCE(econ_score, 0.0)
                 - COALESCE(inflation_score, 0.0)
                 - COALESCE(wti_pressure_score, 0.0)) AS macro_value_support,

                -- Policy side: value prefers mildly hawkish, inflation-conscious stance
                (COALESCE(avg_policy_bias, 0.0)
                 + COALESCE(avg_inflation_risk, 0.0)
                 - COALESCE(avg_growth_risk, 0.0)) AS policy_value_support,

                -- Combine and clip to [-1, 1]
                CASE
                    WHEN (0.6 * (COALESCE(econ_score, 0.0)
                               - COALESCE(inflation_score, 0.0)
                               - COALESCE(wti_pressure_score, 0.0))
                          + 0.4 * (COALESCE(avg_policy_bias, 0.0)
                                 + COALESCE(avg_inflation_risk, 0.0)
                                 - COALESCE(avg_growth_risk, 0.0))) > 1.0
                        THEN 1.0
                    WHEN (0.6 * (COALESCE(econ_score, 0.0)
                               - COALESCE(inflation_score, 0.0)
                               - COALESCE(wti_pressure_score, 0.0))
                          + 0.4 * (COALESCE(avg_policy_bias, 0.0)
                                 + COALESCE(avg_inflation_risk, 0.0)
                                 - COALESCE(avg_growth_risk, 0.0))) < -1.0
                        THEN -1.0
                    ELSE
                        (0.6 * (COALESCE(econ_score, 0.0)
                               - COALESCE(inflation_score, 0.0)
                               - COALESCE(wti_pressure_score, 0.0))
                         + 0.4 * (COALESCE(avg_policy_bias, 0.0)
                                 + COALESCE(avg_inflation_risk, 0.0)
                                 - COALESCE(avg_growth_risk, 0.0)))
                END AS vinv_score
            FROM joined
        )
        SELECT *
        FROM scored
        WHERE date IS NOT NULL
        ORDER BY date
    )
    TO '{out_file.as_posix()}' (FORMAT 'parquet');
    """

    print(f"[VinV v1.0] Using DuckDB: {duckdb_path}")
    print(f"[VinV v1.0] Writing output Parquet: {output_path}")
    con.execute(sql)
    con.close()
    print("[VinV v1.0] Done.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build VinV structural value-support score (v1.0).")
    parser.add_argument("--duckdb", required=True, help="Path to DuckDB file (e.g. data/warehouse/spine.duckdb).")
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output Parquet (e.g. data/processed/p_Equity_US/vinv_signal.parquet).",
    )
    args = parser.parse_args()
    build_vinv_v1(args.duckdb, args.output)


if __name__ == "__main__":
    main()
