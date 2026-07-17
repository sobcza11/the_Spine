from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from spine.jobs.geoscen.eis.exceptions import CredentialError

SECRET_MARKER = "[redacted]"
SECRET_KEY_RE = re.compile(r"(^key$|api[_-]?key|authorization|bearer|cookie|password|secret|token|user[_-]?id)", re.I)
_DOTENV_ATTEMPTED = False


@dataclass(frozen=True, repr=False)
class Credential:
    name: str
    value: str

    def __repr__(self) -> str:
        return f"Credential(name={self.name!r}, value={SECRET_MARKER!r})"

    def __str__(self) -> str:
        return SECRET_MARKER


def load_dotenv_if_available() -> None:
    global _DOTENV_ATTEMPTED
    if _DOTENV_ATTEMPTED:
        return
    _DOTENV_ATTEMPTED = True
    try:
        from dotenv import load_dotenv
    except Exception:
        return
    load_dotenv()


def get_credential(name: str, required: bool = True, *, load_dotenv: bool = False) -> Credential | None:
    if load_dotenv:
        load_dotenv_if_available()
    value = os.getenv(name, "").strip()
    if not value:
        if required:
            raise CredentialError(f"Missing required credential: {name}.", context={"credential": name})
        return None
    return Credential(name=name, value=value)


def get_provider_credentials(
    provider: str,
    specification: Mapping[str, bool] | None,
    *,
    load_dotenv: bool = False,
) -> dict[str, Credential]:
    credentials: dict[str, Credential] = {}
    for name, required in (specification or {}).items():
        credential = get_credential(name, required=required, load_dotenv=load_dotenv)
        if credential is not None:
            credentials[name] = credential
    return credentials


def is_secret_key(key: object) -> bool:
    return bool(SECRET_KEY_RE.search(str(key)))


def redact_value(key: object, value: object) -> object:
    return SECRET_MARKER if is_secret_key(key) else value


def redact_mapping(mapping: Mapping[str, object] | None) -> dict[str, object]:
    redacted: dict[str, object] = {}
    for key, value in (mapping or {}).items():
        if is_secret_key(key):
            redacted[key] = SECRET_MARKER
        elif isinstance(value, Mapping):
            redacted[key] = redact_mapping(value)
        else:
            redacted[key] = value
    return redacted


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    query = urlencode(
        [(key, SECRET_MARKER if is_secret_key(key) else value) for key, value in parse_qsl(parts.query, keep_blank_values=True)],
    )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
