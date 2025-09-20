import psycopg2

# NeonDB connection details
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_t70imvFJbTOW"
DB_HOST = "ep-wild-glade-adu0fglb-pooler.c-2.us-east-1.aws.neon.tech"
DB_PORT = "5432"

def get_connection():
    """Return a live NeonDB connection and cursor."""
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sslmode="require",
        channel_binding="require"
    )
    return conn, conn.cursor()
