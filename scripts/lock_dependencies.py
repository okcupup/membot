"""Record the validated installed dependency graph, including transitive extras.

Run from .venv after intentional dependency upgrades. This is version locking;
image manifests also pin the Python base, independently of package resolution.
"""
from __future__ import annotations

import importlib.metadata as metadata
import tomllib
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name


def main():
    data = tomllib.loads(Path("pyproject.toml").read_text())["project"]
    queue = [*data["dependencies"], *data["optional-dependencies"]["api"],
             *data["optional-dependencies"]["service"]]
    visited, versions = set(), {}
    while queue:
        requirement = Requirement(queue.pop())
        if requirement.marker and not requirement.marker.evaluate({"extra": ""}):
            continue
        name = canonicalize_name(requirement.name)
        distribution = metadata.distribution(name)
        if distribution.version not in requirement.specifier:
            raise ValueError(f"{name} violates {requirement.specifier}")
        versions[name] = distribution.version
        extras = frozenset(requirement.extras) or frozenset([""])
        if (name, extras) in visited:
            continue
        visited.add((name, extras))
        for text in distribution.requires or []:
            child = Requirement(text)
            if child.marker is None or any(child.marker.evaluate({"extra": extra}) for extra in extras):
                child.marker = None
                queue.append(str(child))
    lines = ["# Python 3.11 Linux; validated transitive runtime graph. Regenerate with scripts/lock_dependencies.py."]
    lines.extend(f"{name}=={version}" for name, version in sorted(versions.items()))
    Path("deploy/requirements.lock").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
