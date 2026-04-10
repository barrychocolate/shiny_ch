from pathlib import Path
import pandas as pd
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]

csv_path = BASE_DIR / "data" / "raw" / "BasicCompanyDataAsOneFile-2026-04-01.csv"
parquet_path = BASE_DIR / "data" / "processed" / "companies_house.parquet"

# Start process
start_time = time.perf_counter()

logger.info("Starting CSV → Parquet conversion")
logger.info(f"CSV path: {csv_path}")
logger.info(f"Parquet output path: {parquet_path}")

# Check file exists
if not csv_path.exists():
    logger.error("CSV file not found. Exiting.")
    raise FileNotFoundError(csv_path)

# Load CSV
logger.info("Loading CSV file into DataFrame...")
load_start = time.perf_counter()

df = pd.read_csv(csv_path, low_memory=False, skipinitialspace=True)

load_time = time.perf_counter() - load_start
logger.info(f"CSV loaded successfully in {load_time:.2f} seconds")
logger.info(f"Data shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

# Column diagnostics
logger.info("Inspecting key columns...")

expected_cols = [
    "CompanyNumber",
    "CompanyName",
    "CompanyStatus",
    "CountryOfOrigin",
    "IncorporationDate"
]

missing_cols = [col for col in expected_cols if col not in df.columns]

if missing_cols:
    logger.warning(f"Missing expected columns: {missing_cols}")
else:
    logger.info("All expected columns are present")

# Data cleaning
logger.info("Starting data cleaning...")

# Convert dates
if "IncorporationDate" in df.columns:
    logger.info("Parsing IncorporationDate column...")
    
    df["IncorporationDate"] = pd.to_datetime(
        df["IncorporationDate"],
        format="%d/%m/%Y",   # 👈 key fix
        errors="coerce"
    )

    null_dates = df["IncorporationDate"].isna().sum()
    logger.info(f"Invalid or missing dates: {null_dates:,}")

# Convert to categories (memory optimisation)
for col in ["CompanyStatus", "CountryOrigin"]:
    if col in df.columns:
        logger.info(f"Converting {col} to category dtype")
        df[col] = df[col].astype("category")

# Save to Parquet
logger.info("Writing to Parquet...")

write_start = time.perf_counter()

df.to_parquet(
    parquet_path,
    engine="pyarrow",
    index=False,
    compression="snappy"
)

write_time = time.perf_counter() - write_start
logger.info(f"Parquet file written in {write_time:.2f} seconds")

# Final summary
total_time = time.perf_counter() - start_time

file_size_mb = parquet_path.stat().st_size / (1024 * 1024)

logger.info("Conversion complete")
logger.info(f"Output file size: {file_size_mb:.2f} MB")
logger.info(f"Total processing time: {total_time:.2f} seconds")
