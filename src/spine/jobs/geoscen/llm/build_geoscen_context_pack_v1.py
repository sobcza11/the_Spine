from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_retrieval_pack_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_context_pack_v1.parquet"


def join_top(df: pd.DataFrame, n: int = 8) -> str:
    if df.empty:
        return ""
    rows = []
    for _, r in df.head(n).iterrows():
        rows.append(
            f"- [{r.get('source_family','')}/{r.get('zt_module','')}] "
            f"{r.get('llm_context','')}"
        )
    return "\n".join(rows)


def main():
    df = pd.read_parquet(INPUT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    context_rows = []

    for date, g in df.groupby("date"):
        g = g.sort_values("retrieval_score", ascending=False)

        pmi = g[g["source_family"].eq("PMI")]
        cb = g[g["source_family"].eq("MacroCB")]
        fomc = g[g["source_family"].eq("FOMC")]
        beige = g[g["source_family"].eq("BeigeBook")]

        macro_context = join_top(pmi, 10)
        policy_context = join_top(cb, 10)
        fomc_context = join_top(fomc, 3)
        beige_context = join_top(beige, 3)

        full_context = "\n\n".join(
            [
                "[PMI / Macro Signals]\n" + macro_context,
                "[Central Bank / Policy Signals]\n" + policy_context,
                "[FOMC Evidence]\n" + fomc_context,
                "[Beige Book Evidence]\n" + beige_context,
            ]
        )

        context_rows.append(
            {
                "date": date,
                "module": "GeoScen",
                "layer": "Tier 2.5",
                "reporting_role": "RBL / explanation only",
                "macro_context": macro_context,
                "policy_context": policy_context,
                "fomc_context": fomc_context,
                "beige_context": beige_context,
                "full_context": full_context,
                "version": "geoscen_context_pack_v1",
            }
        )

    out = pd.DataFrame(context_rows).sort_values("date").reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen context pack v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out.tail(5)[["date", "version"]])


if __name__ == "__main__":
    main()

    