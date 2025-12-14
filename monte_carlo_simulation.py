import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# 1️⃣ قراءة الملف المدمج
merged_file = r'.\temp_silver\merged_dataset.parquet'
df = pd.read_parquet(merged_file)

# 2️⃣ إعداد سيناريوهات الطقس
weather_scenarios = {
    'heavy_rain': 0.3,
    'temp_extreme': 0.2,
    'high_humidity': 0.15,
    'low_visibility': 0.25,
    'strong_winds': 0.1
}

n_simulations = 10000
results = []

# 3️⃣ محاكاة مونت كارلو
for i in range(n_simulations):
    scenario_active = {k: np.random.rand() < p for k, p in weather_scenarios.items()}
    congestion_prob = 0.05 + sum(scenario_active.values()) * 0.2
    accident_prob = 0.02 + sum(scenario_active.values()) * 0.1
    results.append({
        'simulation': i+1,
        'congestion_prob': min(congestion_prob, 1.0),
        'accident_prob': min(accident_prob, 1.0),
        **scenario_active
    })

df_results = pd.DataFrame(results)

# 4️⃣ التأكد من وجود المجلد gold_layer
gold_layer_dir = r'.\gold_layer'
os.makedirs(gold_layer_dir, exist_ok=True)

# 5️⃣ حفظ النتائج
gold_csv = os.path.join(gold_layer_dir, 'simulation_results.csv')
df_results.to_csv(gold_csv, index=False)
print(f"✅ Simulation results saved to: {gold_csv}")

# 6️⃣ رسم توزيع احتمالية الازدحام
plt.figure(figsize=(10,6))
plt.hist(df_results['congestion_prob'], bins=50, color='skyblue', edgecolor='black')
plt.title('Distribution of Traffic Congestion Probability')
plt.xlabel('Congestion Probability')
plt.ylabel('Frequency')
plt.grid(True)
plot_path = os.path.join(gold_layer_dir, 'congestion_distribution.png')
plt.savefig(plot_path)
plt.show()
print(f"✅ Congestion distribution plot saved to: {plot_path}")
