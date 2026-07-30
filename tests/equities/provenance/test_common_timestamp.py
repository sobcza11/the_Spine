from datetime import datetime, timezone

import pytest

from spine.equities.provenance._common import parse_utc


@pytest.mark.parametrize(
    "value",
    [
        "2026-07-30T18:29:56Z",
        "2026-07-30T18:29:56.720942Z",
        "2026-07-30T18:29:56+00:00",
        "2026-07-30T18:29:56.720942+00:00",
    ],
)
def test_parse_utc_accepts_governed_iso8601_formats(value):
    parsed = parse_utc(value)
    assert parsed.tzinfo is not None
    assert parsed.utcoffset() == timezone.utc.utcoffset(parsed)
    assert parsed == datetime(2026, 7, 30, 18, 29, 56, parsed.microsecond, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("value", "error"),
    [
        ("not-a-timestamp", "TIMESTAMP_INVALID"),
        ("2026-07-30", "TIMESTAMP_NOT_UTC"),
        ("2026-07-30T18:29:56", "TIMESTAMP_NOT_UTC"),
        ("2026-07-30T18:29:56+01:00", "TIMESTAMP_NOT_UTC"),
        ("2026-07-30T18:29:56-01:00", "TIMESTAMP_NOT_UTC"),
        ("", "TIMESTAMP_INVALID"),
        (None, "TIMESTAMP_INVALID"),
        (datetime(2026, 7, 30, tzinfo=timezone.utc), "TIMESTAMP_INVALID"),
        ("2026-02-30T18:29:56Z", "TIMESTAMP_INVALID"),
        ("2026-07-30T25:29:56Z", "TIMESTAMP_INVALID"),
    ],
)
def test_parse_utc_rejects_invalid_or_unsupported_values(value, error):
    with pytest.raises(ValueError, match=error):
        parse_utc(value)


def test_parse_utc_normalization_is_deterministic():
    trailing_z = parse_utc("2026-07-30T18:29:56.720942Z")
    explicit_offset = parse_utc("2026-07-30T18:29:56.720942+00:00")
    assert trailing_z == explicit_offset
    assert trailing_z.isoformat() == "2026-07-30T18:29:56.720942+00:00"
