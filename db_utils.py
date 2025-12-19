import yaml
import pyodbc
from pathlib import Path


class DatabaseConnector:
    def __init__(self, config_filename: str = "db_cred.yaml"):
        self.cfg_path = Path(__file__).resolve().parent / config_filename
        self.config = self._load_db_config()

    def _load_db_config(self) -> dict:
        config = {
            "server": None,
            "port": "",
            "database": None,
            "username": None,
            "password": None,
            "driver": "FreeTDS",
            "encrypt": "yes",
            "trust_server_certificate": "yes",
            "trusted_connection": False,
        }
        if self.cfg_path.exists():
            with open(self.cfg_path, "r", encoding="utf-8") as file:
                data = yaml.safe_load(file) or {}
                if not isinstance(data, dict):
                    raise ValueError("Database configuration file is malformed.")
                config.update({k: v for k, v in data.items() if v is not None})
        return config

    def create_connection(self):
        if pyodbc is None:
            raise ImportError("pyodbc module is not installed.")

        cfg = self.config

        if not cfg["server"] or not cfg["database"]:
            raise ValueError("Missing required: server or database configuration.")

        server = f"{cfg['server']}:{cfg['port']}" if cfg.get("port") else cfg["server"]

        driver_val = cfg.get("driver")
        if isinstance(driver_val, dict):
            # Handle YAML inline mapping like {ODBC Driver 18 for SQL Server}
            driver_val = next(iter(driver_val.keys()), "")
        driver_str = str(driver_val or "").strip()
        if not driver_str:
            raise ValueError("Missing driver in DB config.")

        parts = [
            f"DRIVER={{{driver_str}}}",
            f"SERVER={server}",
            f"DATABASE={cfg['database']}",
            f"ENCRYPT={cfg['encrypt']}",
            f"TrustServerCertificate={cfg['trust_server_certificate']}",
            "CONNECTION TIMEOUT=30",
        ]

        if cfg.get("trusted_connection"):
            parts.append("Trusted_Connection=yes")
        else:
            if not cfg["username"] or not cfg["password"]:
                raise ValueError("Missing required: username or password configuration.")
            parts.append(f"UID={cfg['username']}")
            parts.append(f"PWD={cfg['password']}")

        connection_string = ";".join(parts) + ";"
        return pyodbc.connect(connection_string)
