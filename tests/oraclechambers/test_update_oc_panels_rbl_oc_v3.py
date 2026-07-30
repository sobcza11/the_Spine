from pathlib import Path

import pandas as pd
import pytest

from spine.jobs.oraclechambers.presentation import EQUITIES_INDUSTRY_RBL_PLACEHOLDER_V1
from spine.jobs.oraclechambers import update_oc_panels_rbl_oc_v3 as panels


def _write(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([row]).to_parquet(path, index=False)


def _setup_sources(root: Path, *, omit_equities_field: str | None = None) -> None:
    base = root / "data/serving"
    common = {
        "date": pd.Timestamp("2026-04-16"),
        "regime_label": "Low-Conviction / Monitoring Regime",
        "regime_confidence": 0.1,
        "dominance_mean": 0.2,
        "signal_strength": 0.3,
        "tone_direction": 0.4,
        "rbl_report_with_regime": "Authoritative source report",
    }
    equities = dict(common)
    equities.pop(omit_equities_field, None)
    _write(base / "equities/equities_serving_v2.parquet", equities)
    for domain, value in (("geoscen", "Macro narrative"), ("fx", "FX narrative"), ("rates", "Rates narrative")):
        _write(base / domain / f"{domain}_serving_v2.parquet", {**common, "rbl_oc": value})


def test_equities_panels_use_presentation_owned_placeholder_without_source_field(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_sources(tmp_path)
    monkeypatch.chdir(tmp_path)

    panels.main()

    for name in ("equities_industry_sectors_panel_v1.html", "equities_panel_v1.html"):
        rendered = (tmp_path / "data/serving/equities" / name).read_text(encoding="utf-8")
        assert EQUITIES_INDUSTRY_RBL_PLACEHOLDER_V1 in rendered
        assert "None" not in rendered
        assert ">nan<" not in rendered
    index_rendered = (
        tmp_path / "data/serving/equities/equities_market_indexes_panel_v1.html"
    ).read_text(encoding="utf-8")
    assert "Broad-market signal layer is not yet complete" in index_rendered


def test_missing_equities_analytical_field_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_sources(tmp_path, omit_equities_field="tone_direction")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(KeyError, match="tone_direction"):
        panels.main()


def test_other_domains_continue_to_render_their_own_narratives(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _setup_sources(tmp_path)
    monkeypatch.chdir(tmp_path)
    panels.main()
    assert "Macro narrative" in (tmp_path / "data/serving/geoscen/geoscen_panel_v1.html").read_text()
    assert "FX narrative" in (tmp_path / "data/serving/fx/fx_panel_v1.html").read_text()
    assert "Rates narrative" in (tmp_path / "data/serving/rates/rates_panel_v1.html").read_text()
