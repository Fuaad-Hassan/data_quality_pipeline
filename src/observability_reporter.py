import os
import logging
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
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

def generate_health_dashboard(output_dir: str = "data/04_audit_reports") -> dict:
    """
    Queries PostgreSQL to generate data observability metrics and 
    saves a high-resolution dashboard as a PNG file.
    """
    logger.info("Starting Data Observability & Health Reporting...")
    engine = get_db_engine()
    
    try:
        # Query 1: Clean Data Count
        clean_query = "SELECT COUNT(*) as count FROM prod_demographics WHERE ingestion_date = CURRENT_DATE"
        clean_df = pd.read_sql(clean_query, engine)
        clean_count = clean_df['count'].iloc[0] if not clean_df.empty else 0
        
        # Query 2: Quarantine Data Breakdown
        quarantine_query = """
            SELECT validation_failure_reason, COUNT(*) as count 
            FROM dead_letter_logs 
            WHERE ingestion_date = CURRENT_DATE 
            GROUP BY validation_failure_reason
        """
        quarantine_df = pd.read_sql(quarantine_query, engine)
        quarantine_count = quarantine_df['count'].sum() if not quarantine_df.empty else 0
        logger.info(f"Retrieved Daily Metrics - Clean: {clean_count} | Quarantined: {quarantine_count}")
        
        # Dashboard Visualizations
        sns.set_theme(style="whitegrid")
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        fig.suptitle(f"Data Pipeline Health Audit - {datetime.now().strftime('%Y-%m-%d')}", fontsize=18, fontweight='bold')
        
        status_labels = ['Clean (Prod)', 'Quarantined (DLQ)']
        status_counts = [clean_count, quarantine_count]
        colors = sns.color_palette("pastel")[0:2]
        
        axes[0].bar(status_labels, status_counts, color=colors)
        axes[0].set_title('Daily Ingestion Volume Status', fontsize=14)
        axes[0].set_ylabel('Record Count', fontsize=12)
    
        for i, v in enumerate(status_counts):
            axes[0].text(i, v + 0.05, str(v), ha='center', va='bottom', fontweight='bold', fontsize=12)
            
        if quarantine_count > 0:
            reasons = quarantine_df['validation_failure_reason']
            counts = quarantine_df['count']
            axes[1].pie(counts, labels=reasons, autopct='%1.1f%%', startangle=140, colors=sns.color_palette("Set2"))
            axes[1].set_title('Quarantine Breakdown by Reason', fontsize=14)
        else:
            axes[1].text(0.5, 0.5, 'No Quarantined Data Today!', ha='center', va='center', fontsize=14, color='green')
            axes[1].axis('off')
            axes[1].set_title('Quarantine Breakdown', fontsize=14)
            
        # Save Dashboard to File
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        filename = f"health_audit_{timestamp}.png"
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, filename)
        
        plt.tight_layout()
        plt.savefig(file_path, dpi=300)
        plt.close()
        
        logger.info(f"Dashboard generated successfully at: {file_path}")
        
        return {
            "status": "SUCCESS",
            "report_path": file_path,
            "metrics": {
                "clean_count": int(clean_count),
                "quarantine_count": int(quarantine_count)
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health reporting failed: {e}")
        return {
            "status": "FAILED",
            "error_message": str(e),
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    generate_health_dashboard()
