import os
import logging
import polars as pl
from dotenv import load_dotenv
from src.ingestion_engine import run_ingestion
from src.validation_engine import run_validation
from src.storage_router import route_to_storage
from src.observability_reporter import generate_health_dashboard

load_dotenv()

# Initialize Global Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MasterController")

def normalize_landing_data(landing_path: str):
    """Normalizes the Census API column names and types to match our schema contract."""
    df = pl.read_parquet(landing_path)
    
    rename_mapping = {}
    if "NAME" in df.columns:
        rename_mapping["NAME"] = "location_identifier"
    if "POP" in df.columns:
        rename_mapping["POP"] = "total_population"
        
    if rename_mapping:
        logger.info(f"Normalizing column names: {rename_mapping}")
        df = df.rename(rename_mapping)
        if "median_household_income" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("median_household_income"))
            
        if "total_population" in df.columns:
             df = df.with_columns(pl.col("total_population").cast(pl.Int64, strict=False))

        if "state" in df.columns:
            df = df.drop("state")
            
        df.write_parquet(landing_path)
        logger.info("Normalization complete.")

def execute_pipeline(source_config: dict):
    """
    Sequentially triggers each phase of the data pipeline, passing state (file paths) 
    between independent, idempotent modules.
    """
    logger.info("==================================================")
    logger.info("INITIATING AUTOMATED DATA QUALITY PIPELINE")
    logger.info("==================================================")
    
    # PHASE 1: Omni-Channel Ingestion
    logger.info(">>> PHASE 1: Omni-Channel Ingestion")
    ingestion_result = run_ingestion(source_config)
    
    if ingestion_result.get("status") != "SUCCESS":
        logger.error(f"Pipeline halted during Phase 1: {ingestion_result.get('error_message')}")
        return
    
    landing_path = ingestion_result.get("landing_file_path")
    logger.info(f"Phase 1 Complete. Data landed at: {landing_path}\n")
    normalize_landing_data(landing_path)
    
    # PHASE 2: Config-Driven Validation
    logger.info(">>> PHASE 2: Config-Driven Validation Engine")
    validation_result = run_validation(landing_path)
    
    if validation_result.get("status") != "SUCCESS":
        logger.error(f"Pipeline halted during Phase 2: {validation_result.get('error_message')}")
        return
        
    clean_path = validation_result.get("clean_data_path")
    quarantine_path = validation_result.get("quarantined_data_path")
    logger.info(f"Phase 2 Complete. Bifurcated data paths acquired.\n")
    
    # PHASE 3: Dual-Target Storage & Routing
    logger.info(">>> PHASE 3: Dual-Target Storage Routing")
    storage_result = route_to_storage(clean_path, quarantine_path)
    
    if storage_result.get("status") != "SUCCESS":
        logger.error(f"Pipeline halted during Phase 3: {storage_result.get('error_message')}")
        return
        
    logger.info("Phase 3 Complete. Database transactions securely committed.\n")
    
    # PHASE 4: Data Observability & Health Reporting
    logger.info(">>> PHASE 4: Data Observability & Health Reporting")
    report_result = generate_health_dashboard()
    
    if report_result.get("status") != "SUCCESS":
        logger.error(f"Pipeline encountered non-fatal error during Phase 4: {report_result.get('error_message')}")
    else:
        logger.info(f"Phase 4 Complete. Dashboard generated at: {report_result.get('report_path')}\n")
        
    logger.info("==================================================")
    logger.info("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
    logger.info("==================================================")

if __name__ == "__main__":
    api_key = os.environ.get("CENSUS_API_KEY", "").strip(".")
    
    config = {
        "source_type": "api",
        "api_url": "https://api.census.gov/data/2019/pep/population",
        "params": {
            "get": "NAME,POP",
            "for": "state:*",
            "key": api_key
        }
    }
    execute_pipeline(config)
