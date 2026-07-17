from __future__ import annotations

from dataclasses import asdict, dataclass
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlsplit

from spine.jobs.geoscen.eis.exceptions import RequestValidationError

FHFA_ATTRIBUTION = "FHFA House Price Index® / FHFA HPI®"
APPROVED_DOMAINS = {"www.fhfa.gov", "fhfa.gov"}
ALLOWED_CONTENT_TYPES = {"text/csv", "application/csv", "text/plain", "application/octet-stream"}
MAX_DATASET_ID_LEN = 80


@dataclass(frozen=True)
class FHFADataset:
    dataset_id: str
    display_name: str
    official_source_identifier: str
    source_url: str
    source_type: str
    file_format: str
    geography_level: str
    index_type: str
    frequency: str
    seasonal_adjustment: str
    expected_columns: tuple[str, ...]
    parser_version: str
    maximum_expected_size: int
    active: bool
    attribution: str = FHFA_ATTRIBUTION

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


NATIONAL_COLUMNS = ("place_name", "place_id", "yr", "period", "index_nsa", "index_sa")
STATE_COLUMNS = ("state", "yr", "qtr", "index_nsa", "index_sa")
METRO_COLUMNS = ("metro_name", "cbsa", "yr", "qtr", "index_nsa")
COUNTY_COLUMNS = ("county_name", "county_fips", "yr", "index_nsa")


DATASETS: Mapping[str, FHFADataset] = MappingProxyType(
    {
        "hpi_national_purchase_only": FHFADataset(
            dataset_id="hpi_national_purchase_only",
            display_name="National purchase-only HPI",
            official_source_identifier="FHFA HPI master CSV",
            source_url="https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv",
            source_type="official_download",
            file_format="csv",
            geography_level="national",
            index_type="purchase-only",
            frequency="monthly",
            seasonal_adjustment="seasonally adjusted and not seasonally adjusted",
            expected_columns=NATIONAL_COLUMNS,
            parser_version="fhfa-hpi-national-v1",
            maximum_expected_size=25_000_000,
            active=True,
        ),
        "hpi_state_all_transactions": FHFADataset(
            dataset_id="hpi_state_all_transactions",
            display_name="State all-transactions HPI",
            official_source_identifier="FHFA quarterly all-transactions state CSV",
            source_url="https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_state.csv",
            source_type="official_download",
            file_format="csv",
            geography_level="state",
            index_type="all-transactions",
            frequency="quarterly",
            seasonal_adjustment="not seasonally adjusted",
            expected_columns=STATE_COLUMNS,
            parser_version="fhfa-hpi-state-v1",
            maximum_expected_size=15_000_000,
            active=True,
        ),
        "hpi_metro_all_transactions": FHFADataset(
            dataset_id="hpi_metro_all_transactions",
            display_name="Metro all-transactions HPI",
            official_source_identifier="FHFA quarterly all-transactions metro CSV",
            source_url="https://www.fhfa.gov/hpi/download/quarterly_datasets/hpi_at_metro.csv",
            source_type="official_download",
            file_format="csv",
            geography_level="metro",
            index_type="all-transactions",
            frequency="quarterly",
            seasonal_adjustment="not seasonally adjusted",
            expected_columns=METRO_COLUMNS,
            parser_version="fhfa-hpi-metro-v1",
            maximum_expected_size=30_000_000,
            active=True,
        ),
        "hpi_county_all_transactions": FHFADataset(
            dataset_id="hpi_county_all_transactions",
            display_name="County all-transactions HPI",
            official_source_identifier="FHFA HPI annual county dataset",
            source_url="https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv",
            source_type="official_download",
            file_format="csv",
            geography_level="county",
            index_type="all-transactions",
            frequency="annual",
            seasonal_adjustment="not seasonally adjusted",
            expected_columns=COUNTY_COLUMNS,
            parser_version="fhfa-hpi-county-v1",
            maximum_expected_size=25_000_000,
            active=True,
        ),
    }
)


def list_datasets(*, active_only: bool = True, geography_level: str | None = None, index_type: str | None = None, frequency: str | None = None) -> list[FHFADataset]:
    rows = []
    for dataset in DATASETS.values():
        if active_only and not dataset.active:
            continue
        if geography_level and dataset.geography_level != geography_level:
            continue
        if index_type and dataset.index_type != index_type:
            continue
        if frequency and dataset.frequency != frequency:
            continue
        rows.append(dataset)
    return sorted(rows, key=lambda item: item.dataset_id)


def normalize_dataset_id(value: object) -> str:
    text = str(value or "").strip().lower()
    if not text or len(text) > MAX_DATASET_ID_LEN or not all(ch.isalnum() or ch in {"_", "-"} for ch in text):
        raise ValueError("dataset_id invalid")
    if text not in DATASETS:
        raise ValueError("dataset_id unsupported")
    dataset = DATASETS[text]
    if not dataset.active:
        raise ValueError("dataset inactive")
    return text


def get_dataset(dataset_id: object) -> FHFADataset:
    return DATASETS[normalize_dataset_id(dataset_id)]


def validate_official_url(url: str) -> None:
    parts = urlsplit(url)
    if parts.scheme != "https" or parts.netloc.lower() not in APPROVED_DOMAINS:
        raise RequestValidationError("FHFA URL is not an approved official source.")
    if ".." in parts.path or not parts.path.startswith("/hpi/download/"):
        raise RequestValidationError("FHFA URL path is not allowlisted.")
