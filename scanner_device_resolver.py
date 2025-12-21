import os
import pathlib
from typing import Any

def config_get(config: dict, *keys: str, default: Any = None) -> Any:
    """Return the first non-None config value for the provided keys."""
    for key in keys:
        if key in config and config[key] is not None:
            return config[key]
    return default


def resolve_scanner_device(config: dict) -> str:   
    configured = config_get(config, "scanner_input_device", "Scanner_input_device")
    if configured and os.path.exists(configured):
        return configured

    preferred = config_get(config, "scanner_device_filter", "Scanner_device_filter")
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


def resolve_user(config: dict, dev_path: str) -> str:
    
    user_map = config.get("scanner_user_map") or {}
    if dev_path in user_map:
        return user_map[dev_path]

    base = os.path.basename(dev_path)
    for key, val in user_map.items():
        key_base = os.path.basename(str(key))
        if base == key_base or base.endswith(key_base):
            return val

    return ""
