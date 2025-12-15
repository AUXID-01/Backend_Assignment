from etl.extract.sheets_extractor import extract_google_sheet

from etl.transform.transform_pipeline import run_students_transform
from etl.load.load_pipeline import run_load_pipeline
from etl.extract.csv_extractor import extract_from_csv






def run_etl_pipeline():
    print("🚀 ETL Pipeline Started")

    # 1️⃣ EXTRACT
    # raw_rows = extract_google_sheet()
    # raw_rows = extract_from_csv("students_large.csv")
    # raw_rows = extract_from_csv("students_messy.csv")
    raw_rows = extract_from_csv("tools/students_kaggle_adapted.csv")
    print(f"📤 Extracted {len(raw_rows)} raw rows")

    # 2️⃣ TRANSFORM
    transform_result = run_students_transform(raw_rows)

    transformed_data = transform_result["data"]
    transform_metrics = transform_result["metrics"]

    print("🧹 Transform Metrics:", transform_metrics)

    # 3️⃣ LOAD
    load_summary = run_load_pipeline(transformed_data)

    print("📥 Load Summary:", load_summary)

    print("✅ ETL Pipeline Completed Successfully")

    return {
        "transform_metrics": transform_metrics,
        "load_summary": load_summary
    }


if __name__ == "__main__":
    run_etl_pipeline()
