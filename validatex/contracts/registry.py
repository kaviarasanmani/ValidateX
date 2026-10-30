"""
Contract registry - a file-based, git-friendly store of versioned data contracts.

Layout::

    <root>/
      customer_master/
        1.0.0.yaml
        1.1.0.yaml
        2.0.0.yaml

Published versions are **immutable**: re-registering identical content is a no-op, but changing the content of
an existing version is rejected. Registering a new version enforces semantic versioning against the previous
latest version, so a breaking change cannot ship under a minor or patch bump.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

from validatex.contracts.diff import ContractDiff, compare_contracts
from validatex.contracts.model import ContractError, DataContract, normalize_version, parse_version


class ContractRegistry:
    """Directory-backed registry of data contracts (commit the directory to version-control it)."""

    def __init__(self, root: Any = "contracts"):
        self.root = Path(root)

    def _dir(self, name: str) -> Path:
        return self.root / name

    def _path(self, name: str, version: Any) -> Path:
        return self._dir(name) / f"{normalize_version(version)}.yaml"

    # -- queries -----------------------------------------------------------

    def list_contracts(self) -> List[str]:
        if not self.root.is_dir():
            return []
        return sorted(p.name for p in self.root.iterdir() if p.is_dir() and any(p.glob("*.yaml")))

    def versions(self, name: str) -> List[str]:
        """All registered versions of *name*, oldest first."""
        d = self._dir(name)
        if not d.is_dir():
            return []
        found = []
        for p in d.glob("*.yaml"):
            try:
                found.append((parse_version(p.stem), normalize_version(p.stem)))
            except ContractError:
                continue
        return [v for _, v in sorted(found)]

    def latest_version(self, name: str) -> Optional[str]:
        v = self.versions(name)
        return v[-1] if v else None

    def get(self, name: str, version: Optional[Any] = None) -> DataContract:
        """Load a specific version (or the latest when *version* is omitted)."""
        resolved = normalize_version(version) if version is not None else self.latest_version(name)
        if resolved is None:
            raise ContractError(f"No contract named '{name}' in registry '{self.root}'.")
        path = self._path(name, resolved)
        if not path.is_file():
            raise ContractError(f"Contract '{name}' has no version {resolved}. Available: {self.versions(name)}")
        return DataContract.load(path)

    def diff(self, name: str, old_version: Any, new_version: Any) -> ContractDiff:
        return compare_contracts(self.get(name, old_version), self.get(name, new_version))

    # -- publishing --------------------------------------------------------

    def register(self, contract: DataContract, enforce_semver: bool = True) -> Path:
        """
        Publish *contract* to the registry and return the file path.

        Raises :class:`ContractError` when the definition is invalid, when an existing version would be modified,
        when the version is not newer than the latest, or (with ``enforce_semver``) when the version bump is
        smaller than the changes since the previous version require.
        """
        contract.ensure_valid()
        path = self._path(contract.name, contract.version)
        if path.is_file():
            existing = DataContract.load(path)
            if existing.content_hash == contract.content_hash:
                return path
            raise ContractError(
                f"Contract '{contract.name}' v{contract.version} is already published with different content. "
                f"Published versions are immutable - bump the version instead."
            )

        previous = self.latest_version(contract.name)
        if previous is not None:
            if parse_version(contract.version) <= parse_version(previous):
                raise ContractError(
                    f"Version {contract.version} must be greater than the latest registered version {previous}."
                )
            if enforce_semver:
                diff = compare_contracts(self.get(contract.name, previous), contract)
                if not diff.version_ok:
                    raise ContractError(
                        f"Version bump {previous} -> {contract.version} is a {diff.actual_bump} bump, but the changes "
                        f"require a {diff.required_bump} bump:\n" + diff.summary()
                    )
        return contract.save(path)
