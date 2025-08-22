import sqlite3

# connect to the database we just created
conn = sqlite3.connect("db/food_wastage.db")
cursor = conn.cursor()

# run a simple query
cursor.execute("SELECT * FROM Providers;")
rows = cursor.fetchall()
print("Providers table:")
for row in rows:
    print(row)

conn.close()
