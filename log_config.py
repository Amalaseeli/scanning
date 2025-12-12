import logging
from config_utils import load_config
import os

config = load_config()
log_file_path = config.get("log_file_path", "scanning/scan_data.log")

os.makedirs(os.path.dirname(log_file_path), exist_ok=True)

logging.basicConfig(
    filename=log_file_path,
    level = logging.INFO,
    format= '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    encoding='utf-8')
