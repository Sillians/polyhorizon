"""Bounded cleanup of a POSIX job and its descendants on failure or cancellation."""
import os
import signal
import subprocess
import threading
import time


def run_owned_process(command, *, env=None, grace_seconds=10):
    process = subprocess.Popen(command, env=env, start_new_session=True)
    previous = None

    def terminate(signum, frame):
        raise InterruptedError("Job received termination signal")

    if threading.current_thread() is threading.main_thread():
        previous = signal.signal(signal.SIGTERM, terminate)
    try:
        code = process.wait()
        if code:
            raise subprocess.CalledProcessError(code, command)
    finally:
        if previous is not None:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
        try:
            os.killpg(process.pid, signal.SIGTERM)
            deadline = time.monotonic() + grace_seconds
            while time.monotonic() < deadline:
                process.poll()  # Reap the parent while checking descendants too.
                try:
                    os.killpg(process.pid, 0)
                except ProcessLookupError:
                    break
                time.sleep(0.05)
            else:
                os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        finally:
            try:
                process.wait(timeout=5)
            finally:
                if previous is not None:
                    signal.signal(signal.SIGTERM, previous)
