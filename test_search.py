import sqlite3
import time

print("Opening database...")

con = sqlite3.connect("search_index.db")
cur = con.cursor()

query = "jay"

start = time.time()

rows = cur.execute("""
SELECT search_text
FROM search_index
WHERE search_index MATCH ?
LIMIT 20
""", (query,)).fetchall()

end = time.time()

print("Results found:", len(rows))
print("Search time:", end - start, "seconds")

for r in rows[:5]:
    print(r)

con.close()