"""Reconcile a named S6 gateway for an external owner with terminal retirement."""

import os
from pathlib import Path
from typing import Literal

from hermes_cli.service_manager import (
    S6ServiceManager,
    S6_SERVICE_PREFIX,
    register_unregistered_profile_gateway,
)
from utils.files import atomic_write_text

_INTENT_FILE = "gateway-lifecycle-intent"


class GatewayRetiredError(Exception):
    """External owners cannot bring a retired gateway back up."""


def profile_gateway_retired(profile_dir: Path) -> bool:
    try:
        return (profile_dir / _INTENT_FILE).read_text(
            encoding="utf-8"
        ).strip() == "retired"
    except FileNotFoundError:
        return False


def reconcile_profile_gateway(
    manager: S6ServiceManager,
    profile: str,
    profile_dir: Path,
    desired: Literal["running", "retired"],
) -> bool:
    """Return True once S6 has settled the intent. Retrying an intent is safe."""
    import fcntl

    name = f"{S6_SERVICE_PREFIX}{profile}"
    intent_file = profile_dir / _INTENT_FILE
    # A separate lock inode survives atomic intent-file replacement and dashboard restart.
    fd = os.open(
        profile_dir / f".{_INTENT_FILE}.lock",
        os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW,
        0o600,
    )
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return False
        try:
            intent = intent_file.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            intent = None

        if desired == "running" and intent == "retired":
            raise GatewayRetiredError
        if intent not in (None, "starting", "running", "retired"):
            raise ValueError("Invalid persisted S6 gateway lifecycle intent")

        if not (manager.scandir / name).is_dir():
            if desired == "retired" and intent is None:
                atomic_write_text(intent_file, "retired\n", mode=0o600, fsync_dir=True)
                return True
            if not register_unregistered_profile_gateway(manager, profile):
                return False

        fields = manager.gateway_supervisor_state(name)
        if fields is None:
            return False
        up, wanted_up = fields
        if intent == "starting" and not wanted_up:
            # This sample may precede the queued 'up'. A new 'up' would make
            # it ambiguous which request the next sample acknowledges.
            return False
        if desired == "running":
            if not wanted_up:
                atomic_write_text(intent_file, "starting\n", mode=0o600, fsync_dir=True)
                manager.start(name, lock_fd=fd)
                return False
            if intent != "running":
                atomic_write_text(intent_file, "running\n", mode=0o600, fsync_dir=True)
            return up

        if intent != "retired":
            atomic_write_text(intent_file, "retired\n", mode=0o600, fsync_dir=True)
        if wanted_up or not (manager.scandir / name / "down").is_file():
            manager.stop(name, persistent=True, lock_fd=fd)
            return False
        return not up and manager.gateway_fully_down(name)
    finally:
        os.close(fd)
