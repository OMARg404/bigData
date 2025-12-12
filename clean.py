import pandas as pd
import numpy as np
import boto3
import io
from dateutil.parser import parse

# ================================
# 🔧 MinIO Connection
# ================================
MINIO_ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "admin"
SECRET_KEY = "admin123"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)

BRONZE = "bronze"
SILVER = "silver"

# ================================
# 📌 Helper — Safe date parsing
# ================================
def safe_parse_date(x):
    try:
        return parse(x, dayfirst=True)
    except:
        return np.nan

# ================================
# 📌 Load CSV from MinIO
# ================================
def load_csv(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))

# ================================
# 📌 Save Parquet to MinIO
# ================================
def save_parquet(df, bucket, key):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
    print(f"✔ Uploaded Parquet → {bucket}/{key}")

# ================================
# 📌 CLEANING LOGIC
# ================================
def clean_weather(df):
    print("\n===== CLEANING WEATHER DATA =====")

    before = len(df)

    # Remove duplicates
    df = df.drop_duplicates()

    # Fix date formats
    df["date_time"] = df["date_time"].astype(str).apply(safe_parse_date)

    # Fix city: fill missing
    df["city"] = df["city"].fillna("London")

    # Numeric columns
    numeric_cols = [
        "temperature_c", "humidity", "rain_mm",
        "wind_speed_kmh", "visibility_m", "air_pressure_hpa"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove extreme outliers
    df = df[(df["temperature_c"] > -20) & (df["temperature_c"] < 60)]
    df = df[(df["humidity"] >= 0) & (df["humidity"] <= 100)]
    df = df[df["wind_speed_kmh"] < 150]
    df = df[df["visibility_m"] < 20000]

    # Fill missing with median
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    after = len(df)

    print(f"Rows before: {before}")
    print(f"Rows after:  {after}")
    print("✔ Weather dataset cleaned successfully!")

    return df, before, after


def clean_traffic(df):
    print("\n===== CLEANING TRAFFIC DATA =====")

    before = len(df)

    # Remove duplicates
    df = df.drop_duplicates()

    # Fix date formats
    df["date_time"] = df["date_time"].astype(str).apply(safe_parse_date)

    # Fill missing city
    df["city"] = df["city"].fillna("London")

    # Fix numeric values
    df["vehicle_count"] = pd.to_numeric(df["vehicle_count"], errors="coerce")
    df["avg_speed_kmh"] = pd.to_numeric(df["avg_speed_kmh"], errors="coerce")
    df["accident_count"] = pd.to_numeric(df["accident_count"], errors="coerce")
    df["visibility_m"] = pd.to_numeric(df["visibility_m"], errors="coerce")

    # Remove invalid speeds (negative)
    df = df[df["avg_speed_kmh"] >= 0]

    # Remove unrealistic values
    df = df[(df["vehicle_count"] > 0) & (df["vehicle_count"] < 15000)]
    df = df[df["accident_count"] < 30]
    df = df[df["visibility_m"] < 20000]

    # Fill missing numeric values
    numeric_cols = ["vehicle_count", "avg_speed_kmh", "accident_count", "visibility_m"]
    df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].median())

    # Fill missing areas
    df["area"] = df["area"].fillna("Unknown")

    after = len(df)

    print(f"Rows before: {before}")
    print(f"Rows after:  {after}")
    print("✔ Traffic dataset cleaned successfully!")

    return df, before, after


# ================================
# 🚀 MAIN SCRIPT
# ================================
print("\n### LOADING RAW FILES FROM BRONZE ###")

weather = load_csv(BRONZE, "weather_raw.csv")
traffic = load_csv(BRONZE, "traffic_raw.csv")

clean_w, w_before, w_after = clean_weather(weather)
clean_t, t_before, t_after = clean_traffic(traffic)

print("\n### UPLOADING CLEANED PARQUET TO SILVER ###")
save_parquet(clean_w, SILVER, "weather_cleaned.parquet")
save_parquet(clean_t, SILVER, "traffic_cleaned.parquet")

print("\n🎉 DONE! Phase 2 Completed Successfully!")
