from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PACKAGE_NAME = "app_template"
PROJECT_NAME = "app-template"
SKIP_DIRS = {".git", ".next", "node_modules", "__pycache__", ".venv", "dist", "build"}
TEXT_EXTENSIONS = {
    ".md",
    ".toml",
    ".py",
    ".tsx",
    ".ts",
    ".js",
    ".json",
    ".yaml",
    ".yml",
    ".ini",
    ".css",
    ".sql",
    ".txt",
}


def to_package_name(raw_name: str) -> str:
    candidate = re.sub(r"[^a-zA-Z0-9]+", "_", raw_name).strip("_").lower()
    if not candidate:
        raise ValueError("Project name must contain letters or digits.")
    if candidate[0].isdigit():
        raise ValueError("Project name cannot start with a digit.")
    return candidate


def to_project_name(package_name: str) -> str:
    return package_name.replace("_", "-")


def replace_text_in_file(path: Path, *, package_name: str, project_name: str) -> None:
    original = path.read_text(encoding="utf-8")
    updated = original.replace(PACKAGE_NAME, package_name).replace(PROJECT_NAME, project_name)
    if updated != original:
        path.write_text(updated, encoding="utf-8")


def iter_text_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.suffix in TEXT_EXTENSIONS or path.name in {".env.example", ".gitignore", "Makefile"}:
            files.append(path)
    return files


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print("Usage: python3 scripts/bootstrap.py <project_name>", file=sys.stderr)
        return 1

    package_name = to_package_name(argv[1])
    project_name = to_project_name(package_name)

    package_dir = ROOT / "backend" / "src" / PACKAGE_NAME
    new_package_dir = ROOT / "backend" / "src" / package_name
    if not package_dir.exists():
        print(f"Template package directory not found: {package_dir}", file=sys.stderr)
        return 1
    if new_package_dir.exists():
        print(f"Target package directory already exists: {new_package_dir}", file=sys.stderr)
        return 1

    shutil.move(str(package_dir), str(new_package_dir))

    for path in iter_text_files(ROOT):
        replace_text_in_file(path, package_name=package_name, project_name=project_name)

    print(f"Bootstrapped template for package '{package_name}'.")
    print("Next steps:")
    print("  1. Copy .env.example to .env")
    print("  2. cd backend && uv sync --extra dev")
    print("  3. make migrate")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
