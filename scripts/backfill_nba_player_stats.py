import datetime
import sys
import os
from tqdm import tqdm
from src.data_pipeline import DataPipeline

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(BASE_DIR)

# Initialize the pipeline
pipeline = DataPipeline()

start_date = datetime.date(2024, 12, 1)
end_date = datetime.date(2025, 5, 12)

all_dates = [start_date + datetime.timedelta(days=i)
             for i in range((end_date - start_date).days + 1)]

for date in tqdm(all_dates, desc="📦 Backfilling stats"):
    try:
        result = pipeline.pull_stats_by_date(date, force=True)
        print(f"✅ {date}: {result.total_rows_processed} rows processed, {result.total_rows_inserted} inserted")
    except Exception as e:
        print(f"❌ Failed on {date}: {e}")
