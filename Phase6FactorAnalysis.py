import os
import pandas as pd
from sklearn.decomposition import FactorAnalysis

# 1️⃣ تحميل البيانات المدمجة
merged_file = r'E:\MY.PROJECT.1\VS\.py\bigData\temp_silver\merged_dataset.parquet'
df = pd.read_parquet(merged_file)

# 2️⃣ اختيار المتغيرات المطلوبة للتحليل
features = [
    'temperature_c',   # من Weather
    'humidity',
    'rain_mm',
    'wind_speed_kmh',
    'visibility_m_x',
    'air_pressure_hpa',
    'vehicle_count',   # من Traffic
    'avg_speed_kmh',
    'accident_count'
]

# تأكد إن الأعمدة موجودة
df_features = df[features].dropna()

# 3️⃣ إنشاء نموذج تحليل العوامل
n_factors = 3  # نريد 1-3 عوامل مخفية
fa = FactorAnalysis(n_components=n_factors, random_state=42)
fa.fit(df_features)

# 4️⃣ الحصول على Factor Loadings (الأوزان)
loadings = pd.DataFrame(fa.components_.T, index=features, columns=[f'Factor_{i+1}' for i in range(n_factors)])

# 5️⃣ حفظ النتائج في Gold layer
gold_dir = r'E:\MY.PROJECT.1\VS\.py\bigData\gold_layer'
os.makedirs(gold_dir, exist_ok=True)
factor_loadings_file = os.path.join(gold_dir, 'factor_loadings.csv')
loadings.to_csv(factor_loadings_file)
print(f"✅ Factor loadings saved to: {factor_loadings_file}")

# 6️⃣ طباعة تقرير تفسير أولي
print("\n📊 Factor Loadings Table:")
print(loadings)

# تلميح للتفسير:
print("\n🔹 Interpretation Hint:")
for i in range(n_factors):
    factor = loadings.iloc[:, i]
    top_vars = factor.abs().sort_values(ascending=False).head(3).index.tolist()
    print(f"Factor_{i+1} likely influenced by: {', '.join(top_vars)}")
