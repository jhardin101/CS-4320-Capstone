from pathlib import Path
import sqlite3

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "data" / "LumbrasGigaBase2025-06.db3"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

def main():
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';")
            tables = [r[0] for r in cur.fetchall()]
            print("Tables:", tables)
            if not tables:
                print("No tables found.")
                return
            tbl = tables[0]
            cur = conn.execute(f"SELECT * FROM {tbl} LIMIT 5")
            rows = cur.fetchall()
            if rows:
                print(f"First {len(rows)} rows from {tbl}:")
                for row in rows:
                    print(dict(row))
            else:
                print(f"No rows in table {tbl}.")
    except sqlite3.Error as e:
        print("SQLite error: ", e)

if __name__ == "__main__":
    main()