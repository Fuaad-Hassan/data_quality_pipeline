CREATE TABLE IF NOT EXISTS prod_demographics (
    id SERIAL PRIMARY KEY,
    location_identifier VARCHAR(255) NOT NULL,
    total_population INT NOT NULL,
    median_household_income NUMERIC(12, 2),
    ingestion_date DATE DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS dead_letter_logs (
    id SERIAL PRIMARY KEY,
    failed_payload JSONB NOT NULL,
    validation_failure_reason VARCHAR(255) NOT NULL,
    ingestion_date DATE DEFAULT CURRENT_DATE,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
