"""
ValidateX Data Contracts - reusable, version-controlled agreements about a dataset.

>>> from validatex.contracts import DataContract, validate_contract, compare_contracts
"""

from validatex.contracts.diff import BREAKING, INFO, NON_BREAKING, Change, ContractDiff, compare_contracts, version_bump
from validatex.contracts.model import (
    ContractError,
    ContractOwner,
    DataContract,
    QualityRule,
    SchemaField,
    Thresholds,
    normalize_version,
    parse_version,
)
from validatex.contracts.registry import ContractRegistry
from validatex.contracts.validator import ContractResult, ThresholdCheck, validate_contract

__all__ = [
    "BREAKING",
    "INFO",
    "NON_BREAKING",
    "Change",
    "ContractDiff",
    "ContractError",
    "ContractOwner",
    "ContractRegistry",
    "ContractResult",
    "DataContract",
    "QualityRule",
    "SchemaField",
    "Thresholds",
    "ThresholdCheck",
    "compare_contracts",
    "normalize_version",
    "parse_version",
    "validate_contract",
    "version_bump",
]
