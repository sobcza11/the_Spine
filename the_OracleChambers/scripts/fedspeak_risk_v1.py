import argparse
from pathlib import Path
import duckdb


def build_fedspeak_risk(duckdb_path: str, output_path: str):

    con = duckdb.connect(duckdb_path)

    out_file = Path(output_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)

    sql = f"""
    COPY (
        WITH event_tone AS (
            SELECT
                event_id,
                AVG(vader_compound) AS avg_tone
            FROM fedspeak.sentiment_scores
            GROUP BY event_id
        ),
        base AS (
            SELECT
                c.event_id,

                -- Derive event_date from filename
                CASE
                    WHEN regexp_matches(c.event_id, '.*([0-9]{{8}}).*') THEN
                        CAST(strptime(regexp_extract(c.event_id, '([0-9]{{8}})', 1), '%Y%m%d') AS TIMESTAMP)
                    WHEN regexp_matches(c.event_id, '.*([0-9]{{6}}).*') THEN
                        CAST(
                            make_date(
                                CAST(substr(regexp_extract(c.event_id, '([0-9]{{6}})', 1), 1, 4) AS INTEGER),
                                CAST(substr(regexp_extract(c.event_id, '([0-9]{{6}})', 1), 5, 2) AS INTEGER),
                                1
                            ) AS TIMESTAMP
                        )
                    ELSE NULL
                END AS event_date,

                COALESCE(e.avg_tone, 0.0) AS avg_tone

            FROM fedspeak.combined_policy_leaf AS c
            LEFT JOIN event_tone AS e
              ON c.event_id = e.event_id
        )
        SELECT
            event_id,
            event_date,

            avg_tone                        AS policy_bias,
            GREATEST(avg_tone, 0.0)         AS inflation_risk,
            GREATEST(-avg_tone, 0.0)        AS growth_risk,

            CASE
                WHEN event_id ILIKE 'beigebook%' OR event_id ILIKE '%summary%'
                    THEN GREATEST(avg_tone, 0.0)
                ELSE NULL
            END AS beige_inflation_risk,

            CASE
                WHEN event_id ILIKE 'beigebook%' OR event_id ILIKE '%summary%'
                    THEN GREATEST(-avg_tone, 0.0)
                ELSE NULL
            END AS beige_growth_risk,

            CASE
                WHEN event_id ILIKE 'beigebook%' OR event_id ILIKE '%summary%'
                    THEN avg_tone
                ELSE NULL
            END AS beige_policy_bias,

            CAST(NULL AS DOUBLE) AS minutes_inflation_risk,
            CAST(NULL AS DOUBLE) AS minutes_growth_risk,
            CAST(NULL AS DOUBLE) AS minutes_policy_bias

        FROM base
    )
    TO '{out_file.as_posix()}' (FORMAT 'parquet');
    """

    print(f"[FedSpeak Risk v1.0] Using DuckDB: {duckdb_path}")
    print(f"[FedSpeak Risk v1.0] Writing output Parquet: {output_path}")

    con.execute(sql)

    print("[FedSpeak Risk v1.0] Done.")


def main():
    parser = argparse.ArgumentParser(description="FedSpeak Risk v1.0")
    parser.add_argument("--duckdb", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    build_fedspeak_risk(args.duckdb, args.output)


if __name__ == "__main__":
    main()

