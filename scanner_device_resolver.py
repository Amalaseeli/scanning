"""
Helper for resolving the scanner input device and default user without
modifying your existing main.py. Import and call from your code.
"""

import os
import pathlib
from typing import Any


def cfg_get(cfg: dict, *keys: str, default: Any = None) -> Any:
    """Return the first non-None config value for the provided keys."""
    for key in keys:
        if key in cfg and cfg[key] is not None:
            return cfg[key]
    return default


def resolve_scanner_device(cfg: dict) -> str:
    """
    Pick a scanner input device path with fallbacks:
      1) Use configured scanner_input_device/Scanner_input_device if it exists.
      2) If not, search /dev/input/by-id for *event-kbd (optionally filtered by scanner_device_filter).
      3) Fallback to the first /dev/input/event*.
      4) Final fallback: /dev/input/event0 (likely will fail loudly if absent).
    """
    configured = cfg_get(cfg, "scanner_input_device", "Scanner_input_device")
    if configured and os.path.exists(configured):
        return configured

    preferred = cfg_get(cfg, "scanner_device_filter", "Scanner_device_filter")
    by_id = pathlib.Path("/dev/input/by-id")
    if by_id.exists():
        candidates = sorted(by_id.glob("*event-kbd"))
        if preferred:
            for path in candidates:
                if preferred.lower() in path.name.lower():
                    return str(path)
        if candidates:
            return str(candidates[0])

    dev_input = pathlib.Path("/dev/input")
    if dev_input.exists():
        for path in sorted(dev_input.glob("event*")):
            return str(path)

    return configured or "/dev/input/event0"


def resolve_user(cfg: dict, dev_path: str) -> str:
    """
    Resolve user name based on config or device path:
      1) If scanner_user_map has an entry for the full path or basename, use that.
      2) If user_id is set, use it.
      3) Otherwise return empty string.
    """
    user_map = cfg.get("scanner_user_map") or {}
    if dev_path in user_map:
        return user_map[dev_path]

    base = os.path.basename(dev_path)
    for key, val in user_map.items():
        key_base = os.path.basename(str(key))
        # Match exact basename or suffix (handles entries like "event-kbd" against "usb-...-event-kbd").
        if base == key_base or base.endswith(key_base):
            return val

    if cfg.get("user_id"):
        return cfg["user_id"]

    return ""
