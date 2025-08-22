# queries.py
# Run: python queries.py
import pandas as pd
from sqlalchemy import create_engine, text

DB_URL = "sqlite:///db/food_wastage.db"
engine = create_engine(DB_URL, connect_args={"check_same_thread": False})

def run_query(sql, params=None):
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})

# 1) Providers & Receivers count per location
def q1_providers_receivers_by_location():
    sql = """
    WITH p AS (SELECT Location, COUNT(*) AS provider_count FROM Providers GROUP BY Location),
         r AS (SELECT Location, COUNT(*) AS receiver_count FROM Receivers GROUP BY Location)
    SELECT COALESCE(p.Location, r.Location) AS Location,
           IFNULL(p.provider_count, 0) AS Provider_Count,
           IFNULL(r.receiver_count, 0) AS Receiver_Count
    FROM p LEFT JOIN r ON p.Location = r.Location
    UNION
    SELECT COALESCE(p.Location, r.Location) AS Location,
           IFNULL(p.provider_count, 0) AS Provider_Count,
           IFNULL(r.receiver_count, 0) AS Receiver_Count
    FROM r LEFT JOIN p ON r.Location = p.Location
    ORDER BY Location;
    """
    return run_query(sql)

# 2) Top providers by total donated quantity
def q2_top_providers_by_quantity():
    sql = """
    SELECT p.Provider_ID, p.Name, p.Location, IFNULL(SUM(fl.Quantity),0) AS Total_Quantity
    FROM Providers p
    LEFT JOIN Food_Listings fl ON p.Provider_ID = fl.Provider_ID
    GROUP BY p.Provider_ID, p.Name, p.Location
    ORDER BY Total_Quantity DESC;
    """
    return run_query(sql)

# 3) Provider contacts in a given location (parameter: location)
def q3_provider_contacts_by_location(location):
    sql = """
    SELECT Provider_ID, Name, Location, Contact
    FROM Providers
    WHERE Location = :location
    ORDER BY Name;
    """
    return run_query(sql, {"location": location})

# 4) Receivers with most claimed quantity (Approved/Completed)
def q4_top_receivers_by_claimed_quantity(limit=10):
    sql = """
    SELECT r.Receiver_ID, r.Name, r.Location,
           IFNULL(SUM(fl.Quantity),0) AS Total_Claimed_Quantity
    FROM Claims c
    JOIN Food_Listings fl ON c.Food_ID = fl.Food_ID
    JOIN Receivers r ON c.Receiver_ID = r.Receiver_ID
    WHERE c.Status IN ('Approved','Completed')
    GROUP BY r.Receiver_ID, r.Name, r.Location
    ORDER BY Total_Claimed_Quantity DESC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})

# 5) Total quantity available across all listings
def q5_total_quantity_available():
    sql = "SELECT IFNULL(SUM(Quantity),0) AS Total_Available FROM Food_Listings;"
    return run_query(sql)

# 6) Locations with highest number of listings (limit)
def q6_top_locations_by_listings(limit=10):
    sql = """
    SELECT p.Location AS Location, COUNT(*) AS Listings
    FROM Food_Listings fl
    JOIN Providers p ON fl.Provider_ID = p.Provider_ID
    GROUP BY p.Location
    ORDER BY Listings DESC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})

# 7) Most common food items (count + total quantity)
def q7_common_food_items():
    sql = """
    SELECT Food_Item, COUNT(*) AS Listings, IFNULL(SUM(Quantity),0) AS Total_Quantity
    FROM Food_Listings
    GROUP BY Food_Item
    ORDER BY Listings DESC, Total_Quantity DESC;
    """
    return run_query(sql)

# 8) How many claims per food item
def q8_claims_per_food_item():
    sql = """
    SELECT fl.Food_ID, fl.Food_Item,
           COUNT(c.Claim_ID) AS Claim_Count
    FROM Food_Listings fl
    LEFT JOIN Claims c ON fl.Food_ID = c.Food_ID
    GROUP BY fl.Food_ID, fl.Food_Item
    ORDER BY Claim_Count DESC;
    """
    return run_query(sql)

# 9) Provider with the highest number of successful claims
def q9_providers_by_successful_claims(limit=10):
    sql = """
    SELECT p.Provider_ID, p.Name, p.Location,
           COUNT(*) AS Completed_Claims
    FROM Claims c
    JOIN Food_Listings fl ON c.Food_ID = fl.Food_ID
    JOIN Providers p ON fl.Provider_ID = p.Provider_ID
    WHERE c.Status IN ('Approved','Completed')
    GROUP BY p.Provider_ID, p.Name, p.Location
    ORDER BY Completed_Claims DESC
    LIMIT :limit;
    """
    return run_query(sql, {"limit": limit})

# 10) Claim status breakdown with percentages
def q10_claim_status_breakdown():
    sql = """
    WITH total AS (SELECT COUNT(*) AS t FROM Claims)
    SELECT Status,
           COUNT(*) AS Count,
           ROUND(100.0 * COUNT(*) / (SELECT t FROM total), 2) AS Percentage
    FROM Claims
    GROUP BY Status;
    """
    return run_query(sql)

# 11) Average quantity claimed per receiver (Approved/Completed)
def q11_avg_qty_claimed_per_receiver():
    sql = """
    WITH rc AS (
      SELECT c.Receiver_ID, SUM(fl.Quantity) AS qty
      FROM Claims c
      JOIN Food_Listings fl ON c.Food_ID = fl.Food_ID
      WHERE c.Status IN ('Approved','Completed')
      GROUP BY c.Receiver_ID
    )
    SELECT ROUND(AVG(qty), 2) AS Avg_Qty_Per_Receiver
    FROM rc;
    """
    return run_query(sql)

# 12) Most claimed food items by claimed quantity
def q12_most_claimed_by_quantity():
    sql = """
    SELECT fl.Food_Item,
           SUM(fl.Quantity) AS Total_Quantity_Claimed,
           COUNT(c.Claim_ID) AS Claim_Count
    FROM Claims c
    JOIN Food_Listings fl ON c.Food_ID = fl.Food_ID
    WHERE c.Status IN ('Approved','Completed')
    GROUP BY fl.Food_Item
    ORDER BY Total_Quantity_Claimed DESC;
    """
    return run_query(sql)

# 13) Total quantity donated by each provider
def q13_total_donated_by_provider():
    sql = """
    SELECT p.Provider_ID, p.Name, p.Location,
           IFNULL(SUM(fl.Quantity),0) AS Total_Donated
    FROM Providers p
    LEFT JOIN Food_Listings fl ON p.Provider_ID = fl.Provider_ID
    GROUP BY p.Provider_ID, p.Name, p.Location
    ORDER BY Total_Donated DESC;
    """
    return run_query(sql)

# 14) Unclaimed items near expiry (no Approved/Completed claims, within N days)
def q14_unclaimed_items_near_expiry(days=1):
    sql = """
    SELECT fl.*
    FROM Food_Listings fl
    LEFT JOIN (
      SELECT DISTINCT Food_ID FROM Claims WHERE Status IN ('Approved','Completed')
    ) cc ON fl.Food_ID = cc.Food_ID
    WHERE cc.Food_ID IS NULL
      AND DATE(fl.Expiry_Date) <= DATE('now', '+' || :days || ' day')
    ORDER BY DATE(fl.Expiry_Date);
    """
    return run_query(sql, {"days": days})

# 15) Providers with no listings
def q15_providers_with_no_listings():
    sql = """
    SELECT p.Provider_ID, p.Name, p.Location, p.Contact
    FROM Providers p
    LEFT JOIN Food_Listings fl ON p.Provider_ID = fl.Provider_ID
    GROUP BY p.Provider_ID, p.Name, p.Location, p.Contact
    HAVING COUNT(fl.Food_ID) = 0
    ORDER BY p.Name;
    """
    return run_query(sql)

# ---- run everything and print nicely ----
def main():
    print("\n1) Providers & Receivers by Location")
    print(q1_providers_receivers_by_location().to_string(index=False))

    print("\n2) Top Providers by Quantity")
    print(q2_top_providers_by_quantity().to_string(index=False))

    print("\n3) Provider Contacts in 'Chennai' (example)")
    print(q3_provider_contacts_by_location("Chennai").to_string(index=False))

    print("\n4) Top Receivers by Claimed Quantity")
    print(q4_top_receivers_by_claimed_quantity(10).to_string(index=False))

    print("\n5) Total Quantity Available")
    print(q5_total_quantity_available().to_string(index=False))

    print("\n6) Top Locations by Listings")
    print(q6_top_locations_by_listings(10).to_string(index=False))

    print("\n7) Most Common Food Items")
    print(q7_common_food_items().to_string(index=False))

    print("\n8) Claims per Food Item")
    print(q8_claims_per_food_item().to_string(index=False))

    print("\n9) Providers by Successful Claims")
    print(q9_providers_by_successful_claims(10).to_string(index=False))

    print("\n10) Claim Status Breakdown")
    print(q10_claim_status_breakdown().to_string(index=False))

    print("\n11) Avg Quantity Claimed per Receiver")
    print(q11_avg_qty_claimed_per_receiver().to_string(index=False))

    print("\n12) Most Claimed Food Items by Quantity")
    print(q12_most_claimed_by_quantity().to_string(index=False))

    print("\n13) Total Quantity Donated by Provider")
    print(q13_total_donated_by_provider().to_string(index=False))

    print("\n14) Unclaimed Items Near Expiry (<= 3 days example)")
    print(q14_unclaimed_items_near_expiry(3).to_string(index=False))

    print("\n15) Providers with No Listings")
    print(q15_providers_with_no_listings().to_string(index=False))

if __name__ == "__main__":
    main()
