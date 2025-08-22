import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# connect to database
engine = create_engine("sqlite:///db/food_wastage.db")

# Load tables into Pandas
providers = pd.read_sql("SELECT * FROM Providers", engine)
receivers = pd.read_sql("SELECT * FROM Receivers", engine)
food_listings = pd.read_sql("SELECT * FROM Food_Listings", engine)
claims = pd.read_sql("SELECT * FROM Claims", engine)

print("Data Loaded:")
print("Providers:", providers.shape)
print("Receivers:", receivers.shape)
print("Food Listings:", food_listings.shape)
print("Claims:", claims.shape)

# 1) Claim status distribution
plt.figure(figsize=(6,4))
sns.countplot(x="Status", data=claims)
plt.title("Claims Status Distribution")
plt.show()

# 2) Food quantity by provider
merged = food_listings.merge(providers, on="Provider_ID")
plt.figure(figsize=(6,4))
sns.barplot(x="Name", y="Quantity", data=merged, estimator=sum)
plt.title("Total Quantity Donated by Each Provider")
plt.xticks(rotation=30)
plt.show()

# 3) Claims per receiver
merged2 = claims.merge(receivers, on="Receiver_ID")
plt.figure(figsize=(6,4))
sns.countplot(x="Name", data=merged2)
plt.title("Claims Count per Receiver")
plt.xticks(rotation=30)
plt.show()

# 4) Food items near expiry
food_listings["Expiry_Date"] = pd.to_datetime(food_listings["Expiry_Date"])
soon_expiring = food_listings[food_listings["Expiry_Date"] <= pd.Timestamp.today() + pd.Timedelta(days=2)]
print("\nFood items expiring in 2 days:")
print(soon_expiring)
