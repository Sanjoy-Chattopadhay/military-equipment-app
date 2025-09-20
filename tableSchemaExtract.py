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


def print_table_schemas():
    conn, cur = get_connection()

    # Get all user tables
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = cur.fetchall()

    for (table,) in tables:
        print(f"\n📌 Table: {table}")
        print("-" * 60)

        # Get column details
        cur.execute("""
            SELECT 
                column_name, 
                data_type, 
                character_maximum_length,
                is_nullable,
                column_default
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position;
        """, (table,))

        cols = cur.fetchall()

        print(f"{'Column':20} {'Type':20} {'MaxLen':10} {'Nullable':10} {'Default'}")
        print("-" * 60)
        for col_name, col_type, maxlen, nullable, default in cols:
            print(f"{col_name:20} {col_type:20} {str(maxlen):10} {nullable:10} {default}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    print("🔎 Fetching schemas from NeonDB...\n")
    print_table_schemas()
    print("\n✅ Done!")
