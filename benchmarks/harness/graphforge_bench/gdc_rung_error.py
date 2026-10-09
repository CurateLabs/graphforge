"""The typed error of a GDC rung input, shared by the readers that raise it."""

from __future__ import annotations


class RungInputError(ValueError):
    """A rung input is malformed or contradicts another, with a typed cause."""

    def __init__(self, cause: str, message: str) -> None:
        super().__init__(message)
        self.cause = cause
