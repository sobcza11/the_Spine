import os
import json
from pathlib import Path
from datetime import datetime, timezone

from openai import OpenAI


ROOT = Path(r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data")
OUT_PATH = ROOT / "llm" / "openai_api_readiness.json"


def main():
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("OPENAI_API_KEY")
    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")

    result = {
        "system": "IsoVector",
        "module": "openai-api-readiness",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "api_key_present": bool(api_key),
        "status": "unknown",
        "error": None,
        "response_preview": None,
    }

    try:
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY not set")

        client = OpenAI(api_key=api_key)

        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "user",
                    "content": "Return JSON only: {\"ok\": true, \"message\": \"api_ready\"}",
                }
            ],
            temperature=0,
        )

        text = response.choices[0].message.content

        parsed = json.loads(text)

        result["status"] = "ok"
        result["response_preview"] = parsed

    except Exception as exc:
        result["status"] = "failed"
        result["error"] = str(exc)

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"Wrote -> {OUT_PATH}")
    print(f"Status -> {result['status']}")
    if result["error"]:
        print(f"Error -> {result['error']}")


if __name__ == "__main__":
    main()
