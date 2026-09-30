"""Merge production defaults without overwriting existing deployment secrets."""

from pathlib import Path
import os
import sys


def entries(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text().splitlines():
        if not line or line.lstrip().startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key in result:
            raise ValueError(f"Duplicate key in {path.name}: {key}")
        result[key] = value
    return result


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    target = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else root / ".env.prod"
    if target.exists() and not target.is_file():
        raise ValueError(f"Not a regular file: {target}")
    defaults = entries(root / ".env.prod.template")
    current = entries(target) if target.exists() else {}
    defaults["DEPLOY_PATH"] = str(root)
    defaults["PREFECT_JOB_ENV_FILE"] = str(target)
    for key, value in current.items():
        defaults[key] = value
    target.write_text("# Generated from .env.prod.template; replace placeholders before deploying.\n"
                      + "\n".join(f"{key}={value}" for key, value in defaults.items()) + "\n")
    os.chmod(target, 0o600)
    missing = sorted(key for key, value in defaults.items() if "REPLACE_ME" in value or "REPLACE_WITH" in value)
    print(f"Prepared {target} ({len(defaults)} variables; {len(missing)} placeholders remain).")
    if missing:
        print("Set these values before deployment: " + ", ".join(missing))


if __name__ == "__main__":
    main()
