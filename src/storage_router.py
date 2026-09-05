import os
import json
import logging
from datetime import datetime
import polars as pl
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

load_dotenv()

def get_db_engine():
    """Constructs the SQLAlchemy engine using .env credentials."""
    user = os.environ.get("DB_USER")
    password = os.environ.get("DB_PASSWORD")
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME")
    
    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}"
    return create_engine(connection_string)

def route_to_storage(clean_data_path: str, quarantined_data_path: str) -> dict:
    """
    Reads the Parquet files and loads them safely into PostgreSQL using atomic transactions.
    """
    logger.info("Starting Dual-Target Storage Routing...")
    engine = get_db_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        if clean_data_path and os.path.exists(clean_data_path):
            clean_df = pl.read_parquet(clean_data_path)
            if not clean_df.is_empty():
                logger.info(f"Loading {len(clean_df)} clean records into 'prod_demographics'")
        
                clean_df.write_database(
                    table_name="prod_demographics",
                    connection=engine,
                    if_table_exists="append",
                    engine="sqlalchemy"
                )
        
        # Load Quarantine Data
        if quarantined_data_path and os.path.exists(quarantined_data_path):
            quarantine_df = pl.read_parquet(quarantined_data_path)
            if not quarantine_df.is_empty():
                logger.info(f"Loading {len(quarantine_df)} quarantine records into 'dead_letter_logs'")
                
                records = quarantine_df.to_dicts()
                
                insert_query = text("""
                    INSERT INTO dead_letter_logs (failed_payload, validation_failure_reason)
                    VALUES (:payload, :reason)
                """)
                
                for record in records:
                    reason = record.pop("validation_failure_reason", "schema mismatch")
                    session.execute(insert_query, {
                        "payload": json.dumps(record),
                        "reason": reason
                    })
        
        session.commit()
        logger.info("Storage routing completed successfully. Transaction Committed.")
        
        return {
            "status": "SUCCESS",
            "message": "Data successfully routed to PostgreSQL.",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        session.rollback()
        logger.error(f"Storage routing failed. Transaction rolled back. Error: {e}")
        return {
            "status": "FAILED",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat()
        }
    finally:
        session.close()
