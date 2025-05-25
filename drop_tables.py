import os
import psycopg2

try:
    conn = psycopg2.connect(
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", 5432),
    )

    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS plays;")
    cur.execute("DROP TABLE IF EXISTS tracks;")
    cur.execute("DROP TABLE IF EXISTS albums;")

    conn.commit()
    cur.close()
    conn.close()

    print("✅ All tables dropped successfully.")

except Exception as e:
    print(f"❌ Error dropping tables: {e}")
