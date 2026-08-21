#!/usr/bin/env python3
"""Generate the Rust dependency license bundle from Cargo metadata."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "THIRD_PARTY_LICENSES.txt"
LICENSE_PREFIXES = (
    "license",
    "licence",
    "copying",
    "copyright",
    "notice",
    "authors",
)
FALLBACK_KIND = {
    "alloc-stdlib": "alloc-no-stdlib",
    "flatbuffers": "apache-2.0",
    "thrift": "apache-2.0",
    "wasip2": "apache-2.0",
}


def cargo_metadata() -> dict[str, object]:
    result = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def license_documents(package: dict[str, object]) -> list[tuple[str, str]]:
    package_root = Path(str(package["manifest_path"])).parent
    candidates: set[Path] = set()
    license_file = package.get("license_file")
    if license_file:
        candidates.add(package_root / str(license_file))
    for candidate in package_root.iterdir():
        if candidate.is_file() and candidate.name.lower().startswith(LICENSE_PREFIXES):
            candidates.add(candidate)

    documents = []
    for candidate in sorted(candidates, key=lambda item: item.name.lower()):
        if not candidate.is_file():
            continue
        content = candidate.read_text(encoding="utf-8-sig", errors="replace")
        content = "\n".join(
            line.rstrip() for line in content.replace("\r\n", "\n").splitlines()
        ).strip()
        if content:
            documents.append((candidate.name, content))
    return documents


def document_key(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def main() -> None:
    metadata = cargo_metadata()
    workspace_members = set(metadata["workspace_members"])
    packages = sorted(
        (
            package
            for package in metadata["packages"]
            if package["id"] not in workspace_members
        ),
        key=lambda package: (package["name"].lower(), package["version"]),
    )

    documents: dict[str, dict[str, object]] = {}
    package_document_keys: dict[str, list[str]] = {}
    package_by_name = {package["name"]: package for package in packages}

    for package in packages:
        package_id = f'{package["name"]} {package["version"]}'
        keys = []
        for filename, content in license_documents(package):
            key = document_key(content)
            record = documents.setdefault(
                key,
                {"content": content, "uses": []},
            )
            record["uses"].append((package_id, filename))
            keys.append(key)
        package_document_keys[package_id] = keys

    apache_key = next(
        key
        for key, record in documents.items()
        if "Apache License" in record["content"]
        and "Version 2.0, January 2004" in record["content"]
        and len(record["content"]) > 8_000
    )
    alloc_package = package_by_name["alloc-no-stdlib"]
    alloc_package_id = f'{alloc_package["name"]} {alloc_package["version"]}'
    alloc_key = package_document_keys[alloc_package_id][0]

    unresolved = []
    for package in packages:
        package_id = f'{package["name"]} {package["version"]}'
        if package_document_keys[package_id]:
            continue
        fallback = FALLBACK_KIND.get(package["name"])
        if fallback == "apache-2.0":
            fallback_key = apache_key
        elif fallback == "alloc-no-stdlib":
            fallback_key = alloc_key
        else:
            unresolved.append(package_id)
            continue
        documents[fallback_key]["uses"].append((package_id, "declared license fallback"))
        package_document_keys[package_id] = [fallback_key]

    if unresolved:
        raise SystemExit(
            "missing license documents and fallbacks for: " + ", ".join(unresolved)
        )

    lines = [
        "THIRD-PARTY RUST DEPENDENCY LICENSES",
        "====================================",
        "",
        "Generated deterministically from Cargo.lock with",
        "scripts/generate_third_party_licenses.py.",
        "",
        "PACKAGE INVENTORY",
        "-----------------",
        "",
    ]
    for package in packages:
        package_id = f'{package["name"]} {package["version"]}'
        authors = "; ".join(package.get("authors") or []) or "not declared"
        repository = package.get("repository") or package.get("homepage") or "not declared"
        keys = ", ".join(key[:12] for key in package_document_keys[package_id])
        lines.extend(
            [
                package_id,
                f'  License expression: {package.get("license") or "not declared"}',
                f"  Authors: {authors}",
                f"  Upstream: {repository}",
                f"  License document IDs: {keys}",
                "",
            ]
        )

    lines.extend(["LICENSE AND NOTICE TEXTS", "------------------------", ""])
    for key, record in sorted(documents.items()):
        uses = sorted(set(record["uses"]))
        lines.append(f"Document ID: {key[:12]}")
        lines.append("Applies to:")
        lines.extend(f"  - {package_id} ({filename})" for package_id, filename in uses)
        lines.extend(["", str(record["content"]), "", "=" * 78, ""])

    OUTPUT.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"wrote {OUTPUT.name}: {len(packages)} packages, {len(documents)} documents")


if __name__ == "__main__":
    main()
