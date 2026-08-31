"""Credential-isolated ESC inputs for the progressive Fly controller.

This module is deliberately import-only.  It does not invoke Pulumi, Fly, or
the progressive attempt controller and is not wired to an operator command.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
import os
from pathlib import Path
import tempfile
from typing import Any

from graphforge_bench.progressive_provider_attempt import (
    SpendAuthorization,
    parse_spend_authorization,
)

FLY_TOKEN_ENV = "FLY_API_TOKEN"
SPEND_AUTHORIZATION_ENV = "GRAPHFORGE_PROGRESSIVE_SPEND_AUTHORIZATION"

_CREDENTIAL_ALIASES = frozenset({"FLY_ACCESS_TOKEN"})
_FORBIDDEN_OVERRIDES = frozenset(
    {
        "ALL_PROXY",
        "DEBUG",
        "FLY_DEBUG",
        "FLY_LOG_LEVEL",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "LOG_LEVEL",
        "NO_PROXY",
        "PULUMI_LOG_LEVEL",
        "RUST_LOG",
    }
)
_PROVIDER_PATH = "/usr/local/bin:/usr/bin:/bin"


class EscCapsuleError(ValueError):
    """A sanitized refusal at the protected environment boundary."""


class _Secret:
    """Small non-printing holder for provider credential material."""

    __slots__ = ("_value",)

    def __init__(self, value: str):
        self._value = value

    def copy(self) -> str:
        return self._value

    def clear(self) -> None:
        self._value = ""

    def __repr__(self) -> str:
        return "<redacted>"


class _ProviderEnvironment(Mapping[str, str]):
    """A subprocess-compatible mapping whose representation stays redacted."""

    __slots__ = ("_fly_token", "_values")

    def __init__(self, fly_token: _Secret, values: Mapping[str, str]):
        self._fly_token = fly_token
        self._values = dict(values)

    def __getitem__(self, name: str) -> str:
        if name == FLY_TOKEN_ENV:
            return self._fly_token.copy()
        return self._values[name]

    def __iter__(self) -> Iterator[str]:
        yield FLY_TOKEN_ENV
        yield from self._values

    def __len__(self) -> int:
        return len(self._values) + 1

    def __repr__(self) -> str:
        return "ProviderEnvironment(FLY_API_TOKEN=<redacted>, isolated_config=True)"


@dataclass(repr=False)
class ProgressiveEscCapsule:
    """Validated ESC authority and an isolated environment for provider calls."""

    _fly_token: _Secret
    _authorization: SpendAuthorization | None
    _temporary: tempfile.TemporaryDirectory[str]
    home: Path
    xdg_config_home: Path
    _authorization_taken: bool = field(default=False, init=False)
    _closed: bool = field(default=False, init=False)
    _cleanup_complete: bool = field(default=False, init=False)

    def __repr__(self) -> str:
        return "ProgressiveEscCapsule(fly_token=<redacted>, authorization=<redacted>)"

    def take_spend_authorization(self) -> SpendAuthorization:
        """Return the parsed authority once, without retaining its encoded form."""
        if self._closed or self._authorization_taken or self._authorization is None:
            raise EscCapsuleError("protected spend authorization is unavailable")
        authorization = self._authorization
        self._authorization = None
        self._authorization_taken = True
        return authorization

    def subprocess_environment(self) -> Mapping[str, str]:
        """Build the complete, minimal environment for one Fly subprocess."""
        if self._closed:
            raise EscCapsuleError("ESC capsule is closed")
        return _ProviderEnvironment(
            self._fly_token,
            {
                "HOME": str(self.home),
                "LANG": "C.UTF-8",
                "LC_ALL": "C.UTF-8",
                "PATH": _PROVIDER_PATH,
                "XDG_CONFIG_HOME": str(self.xdg_config_home),
            },
        )

    def close(self) -> None:
        if self._cleanup_complete:
            return
        self._closed = True
        self._fly_token.clear()
        self._authorization = None
        self._temporary.cleanup()
        self._cleanup_complete = True

    def __enter__(self) -> ProgressiveEscCapsule:
        if self._closed:
            raise EscCapsuleError("ESC capsule is closed")
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()


def _pop_projected_inputs(
    environ: MutableMapping[str, str],
) -> tuple[str | None, str | None, bool]:
    token: str | None = None
    authorization: str | None = None
    rejected = False
    protected = {FLY_TOKEN_ENV, SPEND_AUTHORIZATION_ENV}
    for name in list(environ):
        normalized = name.upper()
        if normalized not in protected | _CREDENTIAL_ALIASES:
            continue
        value = environ.pop(name)
        if name == FLY_TOKEN_ENV and token is None:
            token = value
        elif name == SPEND_AUTHORIZATION_ENV and authorization is None:
            authorization = value
        else:
            rejected = True
    return token, authorization, rejected


def _reject_ambient_overrides(environ: MutableMapping[str, str]) -> None:
    if any(name.upper() in _FORBIDDEN_OVERRIDES for name in environ):
        raise EscCapsuleError("ambient credential or network override is forbidden")


def _validate_token(value: str | None) -> str:
    if (
        not isinstance(value, str)
        or not 1 <= len(value) <= 8192
        or value != value.strip()
        or any(ord(character) < 0x20 or ord(character) == 0x7F for character in value)
    ):
        raise EscCapsuleError("projected Fly credential is unavailable or malformed")
    return value


def _parse_authorization(value: str) -> SpendAuthorization | None:
    """Keep parser exceptions and their protected input outside the public boundary."""
    try:
        return parse_spend_authorization(value)
    except Exception:
        return None


def load_progressive_esc(
    environ: MutableMapping[str, str] | None = None,
) -> ProgressiveEscCapsule:
    """Consume exactly the two protected projections from the process environment."""
    source = os.environ if environ is None else environ
    token_value, authorization_value, rejected_projection = _pop_projected_inputs(source)
    try:
        if rejected_projection:
            raise EscCapsuleError("ambient credential or projected-input override is forbidden")
        _reject_ambient_overrides(source)
        token = _Secret(_validate_token(token_value))
        if not isinstance(authorization_value, str):
            raise EscCapsuleError("protected spend authorization is unavailable")
        authorization = _parse_authorization(authorization_value)
        if authorization is None:
            token.clear()
            raise EscCapsuleError("protected spend authorization is invalid")
    finally:
        token_value = None
        authorization_value = None

    try:
        temporary = tempfile.TemporaryDirectory(prefix="graphforge-progressive-esc-")
    except Exception:
        token.clear()
        raise
    root = Path(temporary.name)
    home = root / "home"
    xdg_config_home = root / "xdg"
    try:
        home.mkdir(mode=0o700)
        xdg_config_home.mkdir(mode=0o700)
    except Exception:
        token.clear()
        temporary.cleanup()
        raise
    return ProgressiveEscCapsule(token, authorization, temporary, home, xdg_config_home)
