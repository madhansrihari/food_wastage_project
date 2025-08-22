import os
import pandas as pd
from sqlalchemy import create_engine

print("▶ Starting db_setup.py ...")

# Ensure folders exist
os.makedirs("db", exist_ok=True)
os.makedirs("data", exist_ok=True)
print("✅ Checked/created folders: db/ and data/")

DB_URL = "sqlite:///db/food_wastage.db"
print(f"▶ Database will be created at: {DB_URL}")
engine = create_engine(DB_URL)

def load_csv(name):
    path = os.path.join("data", name)
    print(f"▶ Trying to load: {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"❌ Missing file: {path}")
    df = pd.read_csv(path)
    print(f"✅ Loaded {name} with {len(df)} rows")
    return df

def main():
    # Load CSV files
    providers = load_csv("providers_data.csv")
    receivers = load_csv("receivers_data.csv")
    food_listings = load_csv("food_listings_data.csv")
    claims = load_csv("claims_data.csv")

    # Save into database
    print("▶ Inserting data into SQLite database ...")
    providers.to_sql("Providers", engine, if_exists="replace", index=False)
    receivers.to_sql("Receivers", engine, if_exists="replace", index=False)
    food_listings.to_sql("Food_Listings", engine, if_exists="replace", index=False)
    claims.to_sql("Claims", engine, if_exists="replace", index=False)

    print("🎉 SUCCESS: Database created at db/food_wastage.db and data inserted.")

if __name__ == "__main__":
    print("▶ Running main() ...")
    main()
