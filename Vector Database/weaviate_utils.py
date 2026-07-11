import json
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def monitor_progress(total: int, current: int, batch_size: int) -> None:
    """Monitor and report progress of batch processing."""
    progress = (current / total) * 100
    logging.info(f"Progress: {progress:.2f}% ({current}/{total} reviews, batch size {batch_size})")