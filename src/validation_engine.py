import os
import yaml
import logging
from datetime import datetime
import polars as pl

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def load_schema(schema_file: str) -> dict:
    """Loads the YAML schema contract into a Python dictionary."""
    with open(schema_file, 'r') as f:
        return yaml.safe_load(f)

def build_validation_expressions(schema_rules: dict) -> list[pl.Expr]:
    """
    Dynamically constructs Polars expressions based on YAML schema rules.
    """
    exprs = []
    for col_name, rules in schema_rules.items():
        is_nullable = rules.get("is_nullable", True)
        if not is_nullable:
            exprs.append(pl.col(col_name).is_not_null())

        min_val = rules.get("min_value")
        if min_val is not None:
            if is_nullable:
                exprs.append(pl.col(col_name).is_null() | (pl.col(col_name) >= min_val))
            else:
                exprs.append(pl.col(col_name) >= min_val)
                
    return exprs

def run_validation(landing_file_path: str, schema_file: str = "config/schema_contracts.yaml") -> dict:
    """
    Executes the Config-Driven Validation Engine.
    Reads landing data lazily from Parquet, applies dynamic rules, and bifurcates the data.
    """
    logger.info(f"Starting validation for {landing_file_path} using schema {schema_file}")
    
    try:
        schema_config = load_schema(schema_file)
        schema_rules = schema_config.get("schema_rules", {})
        lf = pl.scan_parquet(landing_file_path)

        val_exprs = build_validation_expressions(schema_rules)
        
        if not val_exprs:
            logger.warning("No validation rules found. All data will be marked as clean.")
            combined_expr = pl.lit(True)
        else:
            combined_expr = val_exprs[0]
            for expr in val_exprs[1:]:
                combined_expr = combined_expr & expr
        clean_lf = lf.filter(combined_expr)
        quarantine_lf = lf.filter(~combined_expr).with_columns(
            pl.lit("schema mismatch").alias("validation_failure_reason")
        )
        
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        base_name = os.path.basename(landing_file_path).replace("landing_", "").replace(".parquet", "")
        
        clean_dir = "data/02_clean"
        quarantine_dir = "data/03_quarantine"
        
        os.makedirs(clean_dir, exist_ok=True)
        os.makedirs(quarantine_dir, exist_ok=True)
        
        clean_path = os.path.join(clean_dir, f"clean_{base_name}_{timestamp}.parquet")
        quarantine_path = os.path.join(quarantine_dir, f"quarantine_{base_name}_{timestamp}.parquet")

        clean_lf.sink_parquet(clean_path)
        quarantine_lf.sink_parquet(quarantine_path)
        
        logger.info(f"Validation complete. Clean: {clean_path} | Quarantined: {quarantine_path}")

        return {
            "status": "SUCCESS",
            "clean_data_path": clean_path,
            "quarantined_data_path": quarantine_path,
            "validation_timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Validation engine failed: {e}")
        return {
            "status": "FAILED",
            "error_message": str(e),
            "validation_timestamp": datetime.now().isoformat()
        }
