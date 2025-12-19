import os
import json

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "config.json")

def load_config():
    try:
        with open (CONFIG_PATH, "r") as file:
            config_credentials = json.load(file)

        # Device_id/Table_name/etc. are required; Scanner_input_device is now optional
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

        # Connection string is now optional (can be provided via db_cred.yaml)
        # if neither is present, DB code will raise a clearer error later.
        
        
        return config_credentials
    
    except FileNotFoundError:
        raise FileNotFoundError("Configuration file not found.")
        return {}
    
