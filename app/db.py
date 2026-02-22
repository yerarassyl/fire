import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "fire_db"),
        user=os.getenv("DB_USER", "fire_user"),
        password=os.getenv("DB_PASS", "fire_pass"),
    )