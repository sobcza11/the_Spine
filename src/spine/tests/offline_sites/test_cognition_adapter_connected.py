from pathlib import Path
import json


ROOT = Path(
    r"C:\Users\Rand Sobczak Jr\_rts\3_AI\the_Spine\data\review_runtime"
)


DOMAINS = {
    "rates": ["AU", "CA", "DE", "JP", "UK", "CH", "IT", "US", "CN"],
    "fx": ["AUDUSD", "EURUSD", "GBPUSD", "USDCAD", "USDCHF", "USDJPY"],
    "equities_index": ["SPY", "QQQ", "DIA", "ITOT", "MDY", "IWM"],
    "equities_sector": [
        "XLB", "XLC", "XLE", "XLF", "XLI",
        "XLK", "XLP", "XLRE", "XLU", "XLV", "XLY",
    ],
}


VALID_STATUSES = {
    "system_generated",
    "partial_system_generated",
    "manual_placeholder",
    "missing_source",
}


def test_cognition_source_inventory_exists():
    p = ROOT / "cognition_source_inventory.json"

    assert p.exists()

    d = json.loads(
        p.read_text(encoding="utf-8")
    )

    assert d["module"] == "cognition-source-inventory"
    assert "rates" in d["inventory"]
    assert "fx" in d["inventory"]
    assert "equities_index" in d["inventory"]
    assert "equities_sector" in d["inventory"]


def test_connected_payloads_exist():
    for domain, instruments in DOMAINS.items():
        for instrument in instruments:
            p = ROOT / domain / instrument / "content.json"

            assert p.exists(), f"Missing {p}"

            d = json.loads(
                p.read_text(encoding="utf-8")
            )

            assert d["domain"] == domain
            assert d["instrument"] == instrument
            assert d["source_status"] in VALID_STATUSES
            assert isinstance(d["source_files"], list)
            assert isinstance(d["zt"], list)
            assert isinstance(d["rbl"], list)
            assert len(d["zt"]) >= 1
            assert len(d["rbl"]) >= 1
            assert d["governance"]["writeback_allowed"] is False


def test_rendered_pages_show_source_status():
    for domain, instruments in DOMAINS.items():
        for instrument in instruments:
            p = ROOT / domain / instrument / "index.html"

            assert p.exists(), f"Missing {p}"

            html = p.read_text(encoding="utf-8")

            assert "SOURCE STATUS:" in html
            assert "Connected Source Files" in html
            assert "Missing Source Files" in html
            assert "Zₜ Output" in html
            assert "RBL Output" in html


def test_review_index_exists():
    p = ROOT / "index.html"

    assert p.exists()

    html = p.read_text(encoding="utf-8")

    assert "CONNECTED COGNITION REVIEW RUNTIME" in html
    assert "RATES" in html
    assert "FX" in html
    assert "EQUITIES INDEX" in html
    assert "EQUITIES SECTOR" in html
