"""Install/uninstall the per-user macOS LaunchAgent; does not change power settings."""
import argparse
import os
from pathlib import Path
import plistlib
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
LABEL = "com.polyhorizon.market-supervisor"


def definition():
    return {"Label": LABEL, "ProgramArguments": [str(ROOT / ".venv/bin/python"), "-m", "scripts.ops.market_supervisor"],
            "WorkingDirectory": str(ROOT), "RunAtLoad": True, "KeepAlive": True,
            "ThrottleInterval": 60, "EnvironmentVariables": {"PYTHONPATH": str(ROOT),
                "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"},
            "StandardOutPath": str(ROOT / "artifacts/market-supervisor/launchd.log"),
            "StandardErrorPath": str(ROOT / "artifacts/market-supervisor/launchd-error.log")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--uninstall", action="store_true")
    parser.add_argument("--preview", action="store_true")
    args = parser.parse_args()
    if args.preview:
        print(plistlib.dumps(definition()).decode())
        return
    if sys.platform != "darwin":
        parser.error("LaunchAgent installation requires macOS")
    target = Path.home() / "Library/LaunchAgents" / (LABEL + ".plist")
    domain = f"gui/{os.getuid()}"
    subprocess.run(["launchctl", "bootout", domain + "/" + LABEL], capture_output=True)
    # bootout may return before the previous process has fully left the domain.
    for _ in range(20):
        if subprocess.run(["launchctl", "print", domain + "/" + LABEL], capture_output=True).returncode:
            break
        time.sleep(0.5)
    else:
        raise RuntimeError("Previous supervisor still unloading; retry installation shortly")
    if args.uninstall:
        target.unlink(missing_ok=True)
        print("Supervisor disabled; containers and data were not removed.")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    (ROOT / "artifacts/market-supervisor").mkdir(parents=True, exist_ok=True, mode=0o700)
    target.write_bytes(plistlib.dumps(definition()))
    target.chmod(0o600)
    subprocess.run(["launchctl", "bootstrap", domain, str(target)], check=True)
    print("Installed " + LABEL + "; next eligible session will be managed automatically.")


if __name__ == "__main__":
    main()
