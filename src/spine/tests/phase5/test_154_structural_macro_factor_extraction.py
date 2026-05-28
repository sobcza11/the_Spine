from pathlib import Path
import json

P = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\phase5\structural_macro_factor_extraction.json"
)

def test_structural_macro_factor_extraction():
    assert P.exists()

    d = json.loads(
        P.read_text(encoding="utf-8")
    )

    assert d["module"] == "structural-macro-factor-extraction"
    assert d["structural_macro_factor_extraction_enabled"] is True
    assert d["factor_domain_count"] >= 7

    assert "hidden_liquidity_pressure" in d["factor_domains"]
    assert "systemic_volatility_pressure" in d["factor_domains"]

    assert d["factor_contract"]["latent_factor_detection_required"] is True
    assert d["governance"]["unsupported_factor_promotion_forbidden"] is True
    assert d["governance"]["llm_writeback_allowed"] is False
