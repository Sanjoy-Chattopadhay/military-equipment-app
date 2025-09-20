# Now create a script to load CSV data into the SQLite database
import sqlite3
import pandas as pd
import os
from typing import Dict, List


def load_csv_to_sqlite_table(csv_file_path: str, table_name: str, conn: sqlite3.Connection) -> bool:
    """Load a CSV file into a specific SQLite table"""
    try:
        if not os.path.exists(csv_file_path):
            print(f"⚠️  CSV file not found: {csv_file_path}")
            return False

        # Read CSV
        df = pd.read_csv(csv_file_path)
        print(f"📥 Loading {len(df)} records from {os.path.basename(csv_file_path)} into {table_name}")

        # Load into SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"✅ Successfully loaded {len(df)} records into {table_name}")
        return True

    except Exception as e:
        print(f"❌ Error loading {csv_file_path}: {str(e)}")
        return False


def create_data_loader_script():
    """Create a comprehensive data loading script"""

    script_content = '''
import sqlite3
import pandas as pd
import os
from typing import Dict, List

def load_csv_to_sqlite_table(csv_file_path: str, table_name: str, conn: sqlite3.Connection) -> bool:
    """Load a CSV file into a specific SQLite table"""
    try:
        if not os.path.exists(csv_file_path):
            print(f"⚠️  CSV file not found: {csv_file_path}")
            return False

        # Read CSV
        df = pd.read_csv(csv_file_path)
        print(f"📥 Loading {len(df)} records from {os.path.basename(csv_file_path)} into {table_name}")

        # Clean data for SQLite compatibility
        # Convert boolean columns (PostgreSQL TRUE/FALSE to SQLite 1/0)
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace({'True': '1', 'False': '0', 'TRUE': '1', 'FALSE': '0'})

        # Load into SQLite
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"✅ Successfully loaded {len(df)} records into {table_name}")
        return True

    except Exception as e:
        print(f"❌ Error loading {csv_file_path}: {str(e)}")
        return False

def load_all_csv_data(csv_folder_path: str = "csv_data"):
    """Load all CSV files into SQLite database"""

    # Table mapping - adjust these file names to match your actual CSV files
    csv_table_mapping = {
        'jobcard.csv': 'jobcard',
        'jobcarddetails.csv': 'jobcarddetails', 
        'teqptrecord.csv': 'teqptrecord',
        'tfaults.csv': 'tfaults',
        'tssstockmaster.csv': 'tssstockmaster',
        'tsstransactionregister.csv': 'tsstransactionregister',
        'tsubcat.csv': 'tsubcat',
        'tuserunit.csv': 'tuserunit'
    }

    print("🚀 Starting CSV data loading process...")
    print(f"📁 Looking for CSV files in: {os.path.abspath(csv_folder_path)}")

    # Connect to SQLite database
    conn = sqlite3.connect('equipment_database.db')

    loaded_tables = []
    failed_tables = []

    for csv_file, table_name in csv_table_mapping.items():
        csv_path = os.path.join(csv_folder_path, csv_file)

        if load_csv_to_sqlite_table(csv_path, table_name, conn):
            loaded_tables.append(table_name)
        else:
            failed_tables.append(table_name)

    conn.close()

    print("\\n" + "="*60)
    print("📊 LOADING SUMMARY:")
    print("="*60)
    print(f"✅ Successfully loaded tables: {loaded_tables}")
    if failed_tables:
        print(f"❌ Failed to load tables: {failed_tables}")
    print(f"\\n🎉 Database loading completed! ({len(loaded_tables)}/{len(csv_table_mapping)} tables loaded)")

def verify_database():
    """Verify the database structure and data"""
    conn = sqlite3.connect('equipment_database.db')
    cursor = conn.cursor()

    print("\\n🔍 DATABASE VERIFICATION:")
    print("="*50)

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()

    for table in tables:
        table_name = table[0]

        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]

        # Get sample data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 1")
        sample = cursor.fetchone()

        print(f"📋 {table_name:20} | {count:8} rows | Sample: {str(sample)[:50]}...")

    conn.close()

if __name__ == "__main__":
    print("🏗️  Equipment Database CSV Loader")
    print("="*50)

    # Load CSV data (adjust path as needed)
    csv_folder = input("Enter CSV folder path (or press Enter for 'csv_data'): ").strip()
    if not csv_folder:
        csv_folder = "csv_data"

    load_all_csv_data(csv_folder)
    verify_database()
'''

    with open('load_csv_data.py', 'w', encoding='utf-8') as f:
        f.write(script_content)

    print("📝 Created 'load_csv_data.py' script")
    print("\nTo use the data loader:")
    print("1. Place your CSV files in a folder (e.g., 'csv_data')")
    print("2. Run: python load_csv_data.py")
    print("3. Enter the path to your CSV folder when prompted")


create_data_loader_script()