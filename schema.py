import pandas as pd
from sqlalchemy import create_engine

# Neon DB connection
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ZgkW9VU8dBcY@ep-holy-cell-a7vl9851-pooler.ap-southeast-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


def print_table_schemas():
    """Print schemas for all tables"""
    try:
        engine = create_engine(NEON_CONNECTION_STRING)

        tables = ['tuserunit', 'tsubcat', 'teqptrecord', 'jobcard', 'jobcarddetails', 'tfaults']

        for table in tables:
            print(f"\n📋 {table.upper()} SCHEMA:")
            print("=" * 50)

            # Get column info
            df = pd.read_sql(f"""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = '{table}'
                ORDER BY ordinal_position
            """, engine)

            for _, row in df.iterrows():
                nullable = "NULL" if row['is_nullable'] == 'YES' else "NOT NULL"
                default = f" DEFAULT {row['column_default']}" if row['column_default'] else ""
                print(f"  {row['column_name']:<20} {row['data_type']:<15} {nullable}{default}")

    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    print_table_schemas()