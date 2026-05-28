from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

RETRIEVAL_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_retrieval_pack_v1.parquet"
FINBERT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_finbert_tone_pack_v1.parquet"
NUANCE_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_finbert_tone_nuance_pack_v1.parquet"

OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_signal_ranking_v1.parquet"


def norm_abs(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce").fillna(0).abs()
    mx = x.max()
    return x / mx if mx else x


def main():
    retrieval = pd.read_parquet(RETRIEVAL_PATH).copy()
    finbert = pd.read_parquet(FINBERT_PATH).copy()
    nuance = pd.read_parquet(NUANCE_PATH).copy()

    retrieval["date"] = pd.to_datetime(retrieval["date"], errors="coerce")
    finbert["date"] = pd.to_datetime(finbert["date"], errors="coerce")
    nuance["date"] = pd.to_datetime(nuance["date"], errors="coerce")

    # 🔥 CRITICAL FIX — prevent merge explosion
    retrieval = retrieval.drop_duplicates("row_sha256").copy()
    finbert = finbert.drop_duplicates("row_sha256").copy()
    nuance = nuance.drop_duplicates("row_sha256").copy()

    key_cols = ["row_sha256"]

    finbert_keep = key_cols + [
        "finbert_label",
        "finbert_score",
        "finbert_tone_value",
        "finbert_weighted_tone",
    ]

    nuance_keep = key_cols + [
        "finbert_tone_label",
        "finbert_tone_score",
        "finbert_tone_value",
        "finbert_tone_weighted_value",
    ]

    out = retrieval.merge(finbert[finbert_keep], on="row_sha256", how="left")
    out = out.merge(nuance[nuance_keep], on="row_sha256", how="left")

    out["retrieval_score"] = pd.to_numeric(out["retrieval_score"], errors="coerce").fillna(0)
    out["signal_strength"] = pd.to_numeric(out["signal_strength"], errors="coerce").fillna(0)
    out["confidence_norm"] = pd.to_numeric(out["confidence_norm"], errors="coerce").fillna(0)

    out["finbert_weighted_tone"] = pd.to_numeric(out["finbert_weighted_tone"], errors="coerce").fillna(0)
    out["finbert_tone_weighted_value"] = pd.to_numeric(
        out["finbert_tone_weighted_value"], errors="coerce"
    ).fillna(0)

    out["tone_magnitude"] = (
        out["finbert_weighted_tone"].abs()
        + out["finbert_tone_weighted_value"].abs()
    ) / 2

    out["tone_direction"] = out["finbert_weighted_tone"] + out["finbert_tone_weighted_value"]

    out["dominance_score"] = (
        0.40 * out["retrieval_score"]
        + 0.25 * out["signal_strength"]
        + 0.20 * out["tone_magnitude"]
        + 0.15 * out["confidence_norm"]
    )

    out["dominance_rank_date"] = (
        out.groupby("date")["dominance_score"]
        .rank(method="first", ascending=False)
        .astype(int)
    )

    out["dominance_bucket"] = pd.cut(
        out["dominance_rank_date"],
        bins=[0, 5, 15, 50, 10**9],
        labels=["top_5", "top_15", "top_50", "long_tail"],
    ).astype(str)

    out = out.sort_values(
        ["date", "dominance_score"],
        ascending=[True, False],
    ).reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen signal ranking v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(
        out.tail(20)[
            [
                "date",
                "source_family",
                "source_name",
                "dominance_score",
                "dominance_rank_date",
                "finbert_label",
                "finbert_tone_label",
            ]
        ]
    )


if __name__ == "__main__":
    main()
    