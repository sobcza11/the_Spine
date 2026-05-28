from pathlib import Path
import pandas as pd


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_context_pack_tone_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_rbl_template_v1.parquet"

GROWTH_POS = ["growth", "increase", "strong", "expansion"]
GROWTH_NEG = ["decline", "weak", "slow", "contraction"]

INFL_POS = ["inflation", "rising", "pressure", "increase"]
INFL_NEG = ["easing", "decline", "cooling"]

POLICY_TIGHT = ["tight", "higher", "restrictive", "hike"]
POLICY_LOOSE = ["cut", "easing", "support", "accommodative"]


def extract_lines(text, include_keywords, max_lines=2):
    lines = text.split("\n")
    selected = []

    for line in lines:
        if any(k.lower() in line.lower() for k in include_keywords):
            cleaned = clean_line(line)

            # skip garbage
            if len(cleaned) < 30:
                continue

            selected.append(cleaned)

        if len(selected) >= max_lines:
            break

    return " ".join(selected)

def build_narrative_bridge(growth_score, infl_score, policy_score):
    divergence = abs(growth_score - infl_score) + abs(growth_score - policy_score)

    if divergence >= 3:
        return (
            "Significant divergence across growth, inflation, and policy signals "
            "suggests a fragile macro environment with elevated instability risk."
        )

    if growth_score < 0 and infl_score > 0:
        return (
            "Weakening growth alongside persistent inflation pressures suggests "
            "a stagflationary dynamic, limiting policy flexibility."
        )

    return (
        "Signals remain relatively aligned, indicating a more stable but still "
        "uncertain macro backdrop."
    )

def compute_signal_score(text, positive_words, negative_words):
    text = text.lower()

    pos = sum(word in text for word in positive_words)
    neg = sum(word in text for word in negative_words)

    return pos - neg


def build_template(row):
    context = row["full_context_tone"]

    # 1. Extract signals FIRST
    pmi_block = extract_lines(context, ["PMI", "demand", "orders"])
    infl_block = extract_lines(context, ["inflation", "price", "commodity", "cost"])
    policy_block = extract_lines(context, ["Central-bank", "policy", "rates"])

    # 2. Compute scores
    growth_score = compute_signal_score(pmi_block, GROWTH_POS, GROWTH_NEG)
    infl_score = compute_signal_score(infl_block, INFL_POS, INFL_NEG)
    policy_score = compute_signal_score(policy_block, POLICY_TIGHT, POLICY_LOOSE)

    divergence_score = abs(growth_score - infl_score) + abs(growth_score - policy_score)

    # 3. Build readable narrative
    growth = f"Growth momentum appears uneven, with signals indicating: {pmi_block}"
    inflation = f"Price dynamics indicate: {infl_block}"
    policy = f"Policy stance reflects: {policy_block}"

    risk = (
        "Divergence between growth, inflation, and policy signals suggests "
        "elevated macro uncertainty and unstable equilibrium."
    )

    overall = build_narrative_bridge(growth_score, infl_score, policy_score)

    # 4. Final output
    rbl = (
        f"Growth: {growth}\n\n"
        f"Inflation: {inflation}\n\n"
        f"Policy: {policy}\n\n"
        f"Risk: {risk}\n\n"
        f"Overall: {overall}"
    )

    return growth, inflation, policy, risk, overall, rbl

def clean_line(line):
    remove_tokens = [
        "[PMI / Macro Signals]",
        "[Central Bank / Policy Signals]",
        "[PMI/PMI]",
        "[MacroCB/RATES]"
    ]

    for token in remove_tokens:
        line = line.replace(token, "")

    return line.strip()


def main():
    df = pd.read_parquet(INPUT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # test first
    df = df.tail(3).copy()

    outputs = []

    for _, row in df.iterrows():
        growth, inflation, policy, risk, overall, rbl = build_template(row)

        outputs.append({
            "date": row["date"],
            "growth_read": growth,
            "inflation_read": inflation,
            "policy_read": policy,
            "risk_read": risk,
            "overall_read": overall,
            "rbl_report": rbl,
            "version": "geoscen_rbl_template_v1"
        })

    out = pd.DataFrame(outputs).sort_values("date").reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen RBL TEMPLATE v1")
    print("output:", OUTPUT_PATH)
    print(out.tail(3)[["date", "rbl_report"]])


if __name__ == "__main__":
    main()
