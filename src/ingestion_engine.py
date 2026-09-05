import os
import glob
import logging
from datetime import datetime
import requests
import polars as pl
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def ingest_from_api(api_url: str, params: dict = None, landing_dir: str = "data/01_landing") -> dict:
    """
    Executes an HTTP GET request to an API, parses Census Bureau style JSON into a Polars DataFrame,
    and lands it as a Parquet file.
    """
    logger.info(f"Starting API ingestion from {api_url}")
    try:
        response = requests.get(api_url, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        if not data or not isinstance(data, list) or len(data) == 0:
            raise ValueError("Invalid or empty data received from API")

        headers = data[0]
        rows = data[1:]
        
        df = pl.DataFrame(rows, schema=headers, orient="row")
        
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        filename = f"landing_api_{timestamp}.parquet"
        landing_file_path = os.path.join(landing_dir, filename)
        
        os.makedirs(landing_dir, exist_ok=True)
        df.write_parquet(landing_file_path)
        
        logger.info(f"Successfully landed API data to {landing_file_path}")
        
        return {
            "status": "SUCCESS",
            "landing_file_path": landing_file_path,
            "source_type": "api",
            "record_count": len(df),
            "ingestion_timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"API Ingestion failed: {e}")
        return {
            "status": "FAILED",
            "error_message": str(e),
            "source_type": "api",
            "ingestion_timestamp": datetime.now().isoformat()
        }

def ingest_from_dropzone(dropzone_dir: str = "data/00_dropzone", landing_dir: str = "data/01_landing") -> dict:
    """
    Scans the dropzone for the latest CSV file, reads it with Polars to validate readability,
    and copies it to the landing directory with a standardized timestamp in Parquet format.
    """
    logger.info(f"Starting Dropzone ingestion from {dropzone_dir}")
    try:
        csv_files = glob.glob(os.path.join(dropzone_dir, "*.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSV files found in {dropzone_dir}")
        
        latest_file = max(csv_files, key=os.path.getmtime)
        logger.info(f"Found latest dropzone file: {latest_file}")
        
        try:
            df = pl.read_csv(latest_file)
        except Exception as e:
            raise ValueError(f"Failed to read CSV file {latest_file}. Encoding or format issue: {e}")
        
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        filename = f"landing_dropzone_{timestamp}.parquet"
        landing_file_path = os.path.join(landing_dir, filename)
        
        os.makedirs(landing_dir, exist_ok=True)
        df.write_parquet(landing_file_path)
        
        logger.info(f"Successfully landed Dropzone data to {landing_file_path}")
        
        return {
            "status": "SUCCESS",
            "landing_file_path": landing_file_path,
            "source_type": "dropzone",
            "record_count": len(df),
            "ingestion_timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Dropzone Ingestion failed: {e}")
        return {
            "status": "FAILED",
            "error_message": str(e),
            "source_type": "dropzone",
            "ingestion_timestamp": datetime.now().isoformat()
        }

def run_ingestion(source_config: Dict[str, Any]) -> dict:
    """
    Main entry point for Phase 1. Routes execution based on source_type.
    """
    source_type = source_config.get("source_type")
    
    if source_type == "api":
        api_url = source_config.get("api_url")
        params = source_config.get("params", {})
        if not api_url:
            return {
                "status": "FAILED",
                "error_message": "Missing 'api_url' in source_config for api ingestion.",
                "source_type": "api",
                "ingestion_timestamp": datetime.now().isoformat()
            }
        return ingest_from_api(api_url=api_url, params=params)
        
    elif source_type == "dropzone":
        dropzone_dir = source_config.get("dropzone_dir", "data/00_dropzone")
        return ingest_from_dropzone(dropzone_dir=dropzone_dir)
        
    else:
        logger.error(f"Unsupported source_type: {source_type}")
        return {
            "status": "FAILED",
            "error_message": f"Unsupported source_type: {source_type}",
            "source_type": str(source_type),
            "ingestion_timestamp": datetime.now().isoformat()
        }
