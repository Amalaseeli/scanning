import os
import yaml
import pyodbc
import logging
from pathlib import Path

class DatabaseConnector:
    def __init__(self, config_filename='db_cred.yaml'):
        self.cfg_path = Path(__file__).resolve().parent/config_filename
        self.config = self._load_db_config()

        def _load_db_config(self) -> dict:
            config={
                "server": None,
                "port":"",
                "database": None,
                "username": None,   
                "password": None,
                "driver": "FreeTDS",
                "encrypt": "yes",
                "trust_serv`er_certificate": "yes"
            }
            if self.cfg_path.exists():
                with open(self.cfg_path, 'r', encoding='utf-8') as file:
                    data = yaml.safe_load(file) or {}
                    if not isinstance(data, dict):
                            raise ValueError("Database configuration file is malformed.")

                    config.update({k: v for k, v in data.items() if v is not None}) 
            return config
        
    def create_connection(self):
         if pyodbc is None:
            raise ImportError("pyodbc module is not installed. Please install it to use database connections.") 
         
         config = self.load_db_config()

         if not config["server"] or not config["database"]:
             raise ValueError("Missing required: server or database configuration.")
         
         server = f"{config["server"]}:{config["port"]}" if config.get("port") else config["server"]
         coonection_str = [
                f"DRIVER={{{config['driver']}}}",
                f"SERVER={server}",
                f"DATABASE={config['database']}",                
                f"ENCRYPT={config['encrypt']}",
                f"TrustServerCertificate={config['trust_server_certificate']}"
                f"CoNNECTION TIMEOUT=30"
         ]

         if config.get("trusted_connection"):
             coonection_str.append("Trusted_Connection=yes")
         else:
                if not config["username"] or not config["password"]:
                    raise ValueError("Missing required: username or password configuration.")
                coonection_str.append(f"UID={config['username']}")
                coonection_str.append(f"PWD={config['password']}")
        
         connection_string = ";".join(coonection_str) + ";"
         return pyodbc.connect(connection_string)
    
    