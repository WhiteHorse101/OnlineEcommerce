import pandas as pd
from sqlalchemy import create_engine

# 📌 Replace with your actual Supabase credentials
user = "postgres"
password = "your-password"
host = "your-project-ref.supabase.co"  # e.g., xyzcompany.supabase.co
port = "5432"
database = "postgres"

# ✅ SQLAlchemy connection
engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")

# 📄 Load your CSVs
basket_df = pd.read_csv("BasketAnalysis.csv")
customer_df = pd.read_csv("CustonerAnalysis.csv")  # fix filename if needed
seasonality_df = pd.read_csv("SeasonalityAnalysis.csv")

# 🔼 Upload to Supabase
basket_df.to_sql("basket_analysis", engine, if_exists="replace", index=False)
customer_df.to_sql("customer_analysis", engine, if_exists="replace", index=False)
seasonality_df.to_sql("seasonality_analysis", engine, if_exists="replace", index=False)

print("All three tables successfully uploaded to Supabase!")
