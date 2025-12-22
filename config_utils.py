import os
import json

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")


def _resolve_path(base_dir: str, path_value):
    """
    Resolve relative paths against the config file directory so service
    working-directory changes do not move data/log files.
    """
    if not path_value or os.path.isabs(path_value):
        return path_value
    return os.path.normpath(os.path.join(base_dir, path_value))


def load_config():
    try:
        base_dir = os.path.dirname(CONFIG_PATH)
        with open(CONFIG_PATH, "r") as file:
            config_credentials = json.load(file)

        required = [
            "Device_id",
            "Starting_entry_no",
            "Table_name",
            "db_save_interval",
            "log_file_path",
        ]

        for cred in required:
            if cred not in config_credentials:
                raise ValueError(f"Missing required config: {cred}")

        # Normalize file paths relative to the config file location
        path_keys = [
            "log_file_path",
            "state_file",
            "spool_file",
            "spool_offset_file",
        ]
        for key in path_keys:
            if key in config_credentials:
                config_credentials[key] = _resolve_path(base_dir, config_credentials[key])

        return config_credentials

    except FileNotFoundError:
        raise FileNotFoundError("Configuration file not found.")
        return {}
