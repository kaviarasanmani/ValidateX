"""JSON report generator."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from validatex.core.result import ValidationResult


class JSONReportGenerator:
    """Write a ValidationResult to a JSON file."""

    def generate(self, result: "ValidationResult", filepath: str) -> None:
        result.to_json_file(filepath)
