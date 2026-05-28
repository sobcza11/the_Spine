from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_signal_ranking_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_rbl_template_v2.parquet"


GROWTH_TERMS = ["PMI", "demand", "orders", "growth", "expansion", "contraction"]
INFLATION_TERMS = ["inflation", "price", "cost", "commodity", "oil", "energy"]
POLICY_TERMS = ["policy", "rates", "central-bank", "tone_diff", "hawkish", "dovish"]
RISK_TERMS = ["geopolitical", "supply chain", "uncertainty", "tariff", "volatility"]


def contains_any(text: str, terms: list[str]) -> bool:
    text = str(text).lower()
    return any(t.lower() in text for t in terms)


def compress(text: str, max_chars: int = 280) -> str:
    text = " ".join(str(text).split())
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rsplit(" ", 1)[0] + "..."


def top_signal(g: pd.DataFrame, terms: list[str], used_ids: set, n: int = 2) -> tuple[str, set]:
    mask = (
        g["llm_context"].fillna("").apply(lambda x: contains_any(x, terms))
        | g["text_excerpt"].fillna("").apply(lambda x: contains_any(x, terms))
        | g["primary_tag"].fillna("").apply(lambda x: contains_any(x, terms))
        | g["numeric_signal_name"].fillna("").apply(lambda x: contains_any(x, terms))
    )

    selected = (
        g[mask & ~g["row_sha256"].isin(used_ids)]
        .sort_values("dominance_score", ascending=False)
        .head(n)
    )

    if selected.empty:
        selected = (
            g[~g["row_sha256"].isin(used_ids)]
            .sort_values("dominance_score", ascending=False)
            .head(1)
        )

    used_ids.update(selected["row_sha256"].dropna().tolist())

    lines = []
    for _, r in selected.iterrows():
        source = f"{r.get('source_family', '')}/{r.get('source_name', '')}"
        ctx = r.get("llm_context", "") or r.get("text_excerpt", "")
        score = pd.to_numeric(r.get("dominance_score", 0), errors="coerce")
        lines.append(f"{source}: {compress(ctx)} [dominance={score:.3f}]")

    return " | ".join(lines), used_ids


def build_scores(g: pd.DataFrame) -> dict:
    top = g.sort_values("dominance_score", ascending=False).head(15)

    tone_direction = pd.to_numeric(top["tone_direction"], errors="coerce").fillna(0).mean()
    dominance_mean = pd.to_numeric(top["dominance_score"], errors="coerce").fillna(0).mean()
    signal_strength = pd.to_numeric(top["signal_strength"], errors="coerce").fillna(0).mean()

    return {
        "tone_direction": tone_direction,
        "dominance_mean": dominance_mean,
        "signal_strength": signal_strength,
    }


def build_overall(scores: dict) -> str:
    if scores["dominance_mean"] >= 0.45 and abs(scores["tone_direction"]) >= 0.35:
        return (
            "Dominant signals show strong narrative pressure, suggesting the macro backdrop "
            "is becoming less balanced and more vulnerable to regime shift."
        )

    if scores["signal_strength"] >= 0.50:
        return (
            "Signal strength is elevated, indicating that the current macro read is being "
            "driven by a concentrated set of high-impact narratives."
        )

    return (
        "Dominant signals remain present but not extreme, suggesting a mixed macro backdrop "
        "that requires continued monitoring rather than a definitive regime call."
    )


def build_rbl_for_date(date, g: pd.DataFrame) -> dict:
    g = g.sort_values("dominance_score", ascending=False)

    used_ids = set()

    growth_signal, used_ids = top_signal(g, GROWTH_TERMS, used_ids)
    inflation_signal, used_ids = top_signal(g, INFLATION_TERMS, used_ids)
    policy_signal, used_ids = top_signal(g, POLICY_TERMS, used_ids)
    risk_signal, used_ids = top_signal(g, RISK_TERMS, used_ids)

    scores = build_scores(g)

    growth = f"Growth momentum is framed by the dominant demand-side evidence: {growth_signal}"
    inflation = f"Inflation pressure is framed by the highest-ranked price and cost evidence: {inflation_signal}"
    policy = f"Policy interpretation is driven by the strongest central-bank and rates evidence: {policy_signal}"
    risk = f"Risk conditions are shaped by the highest-ranked uncertainty and friction signals: {risk_signal}"
    overall = build_overall(scores)

    rbl_report = (
        f"Growth: {growth}\n\n"
        f"Inflation: {inflation}\n\n"
        f"Policy: {policy}\n\n"
        f"Risk: {risk}\n\n"
        f"Overall: {overall}"
    )

    return {
        "date": date,
        "growth_read": growth,
        "inflation_read": inflation,
        "policy_read": policy,
        "risk_read": risk,
        "overall_read": overall,
        "tone_direction": scores["tone_direction"],
        "dominance_mean": scores["dominance_mean"],
        "signal_strength": scores["signal_strength"],
        "rbl_report": rbl_report,
        "version": "geoscen_rbl_template_v2",
    }


def main():
    df = pd.read_parquet(INPUT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # test latest 3 dates first
    dates = sorted(df["date"].dropna().unique())[-3:]

    outputs = []
    for date in dates:
        g = df[df["date"].eq(date)].copy()
        outputs.append(build_rbl_for_date(date, g))

    out = pd.DataFrame(outputs).sort_values("date").reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen RBL TEMPLATE v2")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out[["date", "dominance_mean", "signal_strength", "tone_direction"]])


if __name__ == "__main__":
    main()

    