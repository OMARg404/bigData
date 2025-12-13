import streamlit as st
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

# Paths
merged_file = r'.\temp_silver\merged_dataset.parquet'
monte_carlo_file = r'.\gold_layer\simulation_results.csv'
factor_loadings_file = r'.\gold_layer\factor_loadings.csv'
factor_heatmap_file = r'.\gold_layer\factor_loadings_heatmap.png'

st.set_page_config(page_title="Traffic & Weather Dashboard", layout="wide")
st.title("📊 Traffic & Weather Analysis Dashboard")

# --------------------------------
# 1️⃣ Dataset Statistics
# --------------------------------
st.header("Dataset Statistics")
df = pd.read_parquet(merged_file)

# 🔹 معالجة عمود date_time لتجنب مشاكل Streamlit
if 'date_time' in df.columns:
    df['date_time'] = df['date_time'].astype(str)

st.dataframe(df.head(10))  # عرض أول 10 صفوف

st.subheader("Summary Statistics")
# تحويل الأعمدة غير رقمية لتفادي مشاكل describe
numeric_cols = df.select_dtypes(include=['float64','int64']).columns
st.write(df[numeric_cols].describe())

# --------------------------------
# 2️⃣ Monte Carlo Simulation Results
# --------------------------------
st.header("Monte Carlo Simulation Results")
df_monte = pd.read_csv(monte_carlo_file)
st.dataframe(df_monte.head(10))

# Congestion Probability Distribution
st.subheader("Congestion Probability Distribution")
fig, ax = plt.subplots(figsize=(10,6))
ax.hist(df_monte['congestion_prob'], bins=50, color='skyblue', edgecolor='black')
ax.set_xlabel('Congestion Probability')
ax.set_ylabel('Frequency')
ax.set_title('Distribution of Traffic Congestion Probability')
st.pyplot(fig)

# Accident Probability Distribution
st.subheader("Accident Probability Distribution")
fig2, ax2 = plt.subplots(figsize=(10,6))
ax2.hist(df_monte['accident_prob'], bins=50, color='salmon', edgecolor='black')
ax2.set_xlabel('Accident Probability')
ax2.set_ylabel('Frequency')
ax2.set_title('Distribution of Accident Probability')
st.pyplot(fig2)

# --------------------------------
# 3️⃣ Factor Analysis Insights
# --------------------------------
st.header("Factor Analysis Insights")
df_factors = pd.read_csv(factor_loadings_file, index_col=0)
st.dataframe(df_factors)

# Heatmap
st.subheader("Factor Loadings Heatmap")
if os.path.exists(factor_heatmap_file):
    st.image(factor_heatmap_file, caption="Factor Loadings Heatmap", use_column_width=True)
else:
    st.write("Heatmap file not found. Please generate it first.")

# Optional: Show top influencing variables per factor
st.subheader("Top Variables per Factor")
for factor in df_factors.columns:
    top_vars = df_factors[factor].abs().sort_values(ascending=False).head(3).index.tolist()
    st.write(f"**{factor}** likely influenced by: {', '.join(top_vars)}")
