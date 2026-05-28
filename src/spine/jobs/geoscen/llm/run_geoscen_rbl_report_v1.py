from pathlib import Path
import pandas as pd
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM
import torch


REPO_ROOT = Path.cwd()

INPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_context_pack_tone_v1.parquet"
OUTPUT_PATH = REPO_ROOT / "data/geoscen/llm/geoscen_rbl_report_v1.parquet"

MODEL_NAME = "google/flan-t5-base"


def build_prompt(row):
    return f"""
You are GeoScen, a macro analyst.

STRICT RULES:
- Write EXACTLY 5 sentences
- NO repetition
- NO filler phrases (e.g. "current system conditions")
- Each sentence must introduce NEW information
- Use signals (PMI, Central Bank, Tone)

STYLE:
- Institutional
- Concise
- Analytical (not descriptive)

Context:
{row['full_context_tone']}

Task:
Write a 5 sentence macro narrative explaining:
1. Growth
2. Inflation
3. Policy stance
4. Market interpretation
5. Overall system condition
"""


def main():
    df = pd.read_parquet(INPUT_PATH).copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    device = 0 if torch.cuda.is_available() else -1

    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME)

    if torch.cuda.is_available():
        model = model.to("cuda")

    outputs = []

    df = df.tail(3).copy()

    for _, row in df.iterrows():
        print(f"Generating RBL report for {row['date']}...")
        prompt = build_prompt(row)

        inputs = tokenizer(
            prompt,
            return_tensors="pt",
            truncation=True,
            max_length=1024,
        )

        if torch.cuda.is_available():
            inputs = {k: v.to("cuda") for k, v in inputs.items()}

        with torch.no_grad():
            output_ids = model.generate(
                **inputs,
                max_new_tokens=150,
                do_sample=True,
                temperature=0.7,
                top_p=0.9,
                repetition_penalty=1.2,
            )

        result = tokenizer.decode(output_ids[0], skip_special_tokens=True)

        sentences = result.split(".")
        clean = []
        for s in sentences:
            s = s.strip()
            if s and s not in clean:
                clean.append(s)

        result = ". ".join(clean[:5]) + "."

        outputs.append({
            "date": row["date"],
            "rbl_report": result,
            "version": "geoscen_rbl_report_v1"
        })

    out = pd.DataFrame(outputs).sort_values("date").reset_index(drop=True)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(OUTPUT_PATH, index=False)

    print("OK | GeoScen RBL report v1")
    print("output:", OUTPUT_PATH)
    print("shape:", out.shape)
    print(out.tail(3))


if __name__ == "__main__":
    main()
