from fastapi import FastAPI
import psycopg2

app = FastAPI()

@app.get("/data")
def read_data():
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname="streaming_db",
            user="postgres",
            password="poastgres",
            host="postgres",
            port="5432")
        cur = conn.cursor()
        cur.execute("SELECT * FROM processed_table LIMIT 10")
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        return {"error": str(e)}
