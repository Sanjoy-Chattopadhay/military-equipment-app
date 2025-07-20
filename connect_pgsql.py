import psycopg2
import pandas as pd
from sqlalchemy import create_engine

# Neon DB connection
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ZgkW9VU8dBcY@ep-holy-cell-a7vl9851-pooler.ap-southeast-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


def test_neon_connection():
    """Test connection to Neon DB"""
    try:
        engine = create_engine(NEON_CONNECTION_STRING)

        # Test each table
        tables = ['tuserunit', 'tsubcat', 'teqptrecord', 'jobcard', 'jobcarddetails', 'tfaults']

        for table in tables:
            df = pd.read_sql(f'SELECT COUNT(*) as count FROM "{table}"', engine)
            print(f"✅ {table}: {df['count'][0]} records")

        print("🎉 All tables accessible!")
        return True

    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_neon_connection()
