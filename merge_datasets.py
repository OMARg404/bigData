import pandas as pd
import os

# 🟢 1️⃣ مسارات الملفات المحلية
weather_file = r'.\temp_silver\weather_cleaned.parquet'
traffic_file = r'.\temp_silver\traffic_cleaned.parquet'

# 🟢 2️⃣ قراءة الملفات
try:
    df_weather = pd.read_parquet(weather_file)
    df_traffic = pd.read_parquet(traffic_file)
    print("✅ Files loaded successfully")
except Exception as e:
    print("⚠️ Error reading files:", e)
    exit(1)

# 🟢 3️⃣ دمج البيانات على الأعمدة المشتركة date_time و city
try:
    df_merged = pd.merge(df_weather, df_traffic, on=['date_time', 'city'], how='inner')
    print(f"📊 Merged dataset shape: {df_merged.shape}")
except Exception as e:
    print("⚠️ Error merging datasets:", e)
    exit(1)

# 🟢 4️⃣ حفظ الـ merged dataset في Silver layer (ممكن تختار أي فولدر)
silver_output = r'.\temp_silver\merged_dataset.parquet'
try:
    df_merged.to_parquet(silver_output, index=False)
    print(f"💾 Merged dataset saved to: {silver_output}")
except Exception as e:
    print("⚠️ Error saving merged dataset:", e)




