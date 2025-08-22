import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to database
conn = sqlite3.connect("db/food_wastage.db")

# Load data
providers = pd.read_sql("SELECT * FROM providers", conn)
receivers = pd.read_sql("SELECT * FROM receivers", conn)
food_listings = pd.read_sql("SELECT * FROM food_listings", conn)
claims = pd.read_sql("SELECT * FROM claims", conn)

st.title("🍽️ Food Wastage Management System")

# Show tables
st.subheader("📊 Raw Data from Database")
st.write("### Providers", providers)
st.write("### Receivers", receivers)
st.write("### Food Listings", food_listings)
st.write("### Claims", claims)

# Visualizations
st.subheader("📈 Visualizations")

# Claims Status
fig, ax = plt.subplots()
sns.countplot(x="Status", data=claims, ax=ax)
st.pyplot(fig)

# Food donated per provider
merged = food_listings.merge(providers, on="Provider_ID")
fig, ax = plt.subplots()
sns.barplot(x="Name", y="Quantity", data=merged, estimator=sum, ax=ax)
plt.xticks(rotation=30)
st.pyplot(fig)

# Claims per receiver
merged2 = claims.merge(receivers, on="Receiver_ID")
fig, ax = plt.subplots()
sns.countplot(x="Name", data=merged2, ax=ax)
plt.xticks(rotation=30)
st.pyplot(fig)

# Food expiring soon
st.subheader("⚠️ Food items expiring in 2 days")
food_listings["Expiry_Date"] = pd.to_datetime(food_listings["Expiry_Date"])
soon_expiring = food_listings[food_listings["Expiry_Date"] <= pd.Timestamp.today() + pd.Timedelta(days=2)]
st.write(soon_expiring)

st.success("✅ Dashboard Loaded Successfully!")
