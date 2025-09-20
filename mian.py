# Create an updated version of your main application to use SQLite
import re

# Read the original application code
with open('paste.txt', 'r') as f:
    original_code = f.read()

# Create the updated SQLite version
updated_code = '''
# UPDATED VERSION - SQLite Migration
import plotly.express as px
import math
import streamlit as st
import sqlite3  # CHANGED: Replace psycopg2 with sqlite3
import pandas as pd
from equipment_analytics import generate_equipment_analytics
from journey_recommendations import generate_spare_parts_prediction

# CHANGE: SQLite connection function
def get_connection():
    """Return a SQLite connection."""
    return sqlite3.connect('equipment_database.db')

# Test connection
def test_connection():
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        conn.close()
        return True, f"Connected successfully. Found {len(tables)} tables."
    except Exception as e:
        return False, f"Connection failed: {str(e)}"

# Updated query functions for SQLite
def get_subcategories():
    conn = get_connection()
    df = pd.read_sql("SELECT subcategoryname FROM tsubcat WHERE categoryname = 'B'", conn)
    subcats = df["subcategoryname"].dropna().unique().tolist()
    conn.close()
    return ["All"] + subcats

def get_subcatid(subcategory_name):
    conn = get_connection()
    query = "SELECT subcatid FROM tsubcat WHERE categoryname = 'B' AND subcategoryname = ?"
    result = pd.read_sql(query, conn, params=[subcategory_name])
    conn.close()
    return result['subcatid'].iloc[0] if not result.empty else None

def get_user_units():
    conn = get_connection()
    query = "SELECT userunit_id, userunit_name FROM tuserunit WHERE movedout = 0"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def get_eqpt_records(subcat_id=None, user_unit_id=None, selected_year=None):
    conn = get_connection()

    query = """
            SELECT r.id AS eqptid, r.regnno, r.nomenclature, r.dtofissue, r.inkm, u.userunit_name
            FROM teqptrecord AS r
                LEFT JOIN tuserunit AS u ON r.userunit = u.userunit_id
                LEFT JOIN tsubcat AS s ON r.cat = s.subcatid
            WHERE s.categoryname = 'B'
            """

    filters = []
    params = []

    if subcat_id is not None:
        filters.append("r.cat = ?")
        params.append(int(subcat_id))

    if user_unit_id is not None:
        filters.append("r.userunit = ?")
        params.append(int(user_unit_id))

    if selected_year and selected_year != "All":
        # SQLite date extraction - different from PostgreSQL
        filters.append("CAST(SUBSTR(r.dtofissue, 1, 4) AS INTEGER) >= ?")
        params.append(int(selected_year))

    if filters:
        query += " AND " + " AND ".join(filters)

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def get_equipment_faults(eqpt_id, selected_year=None):
    conn = get_connection()

    query = """
            SELECT r.id AS eqptid, f.faults AS faultdescription
            FROM teqptrecord AS r
                LEFT JOIN jobcard AS jc ON r.id = jc.referid
                LEFT JOIN jobcarddetails AS jcd ON jc.id = jcd.refjobno
                LEFT JOIN tfaults AS f ON jcd.fault = f.faultid
            WHERE r.id = ?
            """

    params = [eqpt_id]

    if selected_year and selected_year != "All":
        # SQLite date handling
        query += " AND CAST(SUBSTR(jc.jobcarddate, 1, 4) AS INTEGER) = ?"
        params.append(int(selected_year))

    query += " ORDER BY jc.jobcarddate DESC"

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

def get_equipment_details(reg_no, selected_year=None):
    conn = get_connection()

    query = """
            SELECT r.regnno, r.nomenclature, jc.jobcarddate, jc.jobcardno, f.faults
            FROM teqptrecord AS r
                LEFT JOIN jobcard AS jc ON r.id = jc.referid
                LEFT JOIN jobcarddetails AS jcd ON jc.id = jcd.refjobno
                LEFT JOIN tfaults AS f ON jcd.fault = f.faultid
            WHERE r.regnno = ?
            """

    params = [reg_no]

    if selected_year:
        query += " AND CAST(SUBSTR(jc.jobcarddate, 1, 4) AS INTEGER) = ?"
        params.append(int(selected_year))

    query += " ORDER BY jc.jobcarddate DESC"

    df = pd.read_sql(query, conn, params=params)
    conn.close()

    if not df.empty:
        return df['regnno'].iloc[0], df['nomenclature'].iloc[0], df
    return None, None, pd.DataFrame()

def get_spare_parts_data(eqpt_id, selected_year=None):
    conn = get_connection()

    query = """
            SELECT tr.partnoid, sm.itemname, tr.issues
            FROM tsstransactionregister AS tr
                LEFT JOIN tssstockmaster AS sm ON tr.partnoid = sm.id
                LEFT JOIN jobcard AS jc ON CAST(tr.refjobid AS INTEGER) = jc.id
            WHERE jc.referid = ?
            """

    params = [eqpt_id]

    if selected_year and selected_year != "All":
        query += " AND CAST(SUBSTR(jc.jobcarddate, 1, 4) AS INTEGER) = ?"
        params.append(int(selected_year))

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df

# Streamlit App Configuration
st.set_page_config(
    page_title="Equipment Management System",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize theme
if "theme" not in st.session_state:
    st.session_state.theme = "light"

# Toggle button
if st.button("🌗 Toggle Theme"):
    st.session_state.theme = "dark" if st.session_state.theme == "light" else "light"

# Apply CSS based on theme
if st.session_state.theme == "dark":
    st.markdown("""
    <style>
    .stApp { background-color: #1e1e1e; color: white; }
    .stSidebar { background-color: #2d2d2d; }
    </style>
    """, unsafe_allow_html=True)

# Main Application
def main():
    st.title("🔧 Equipment Management System")
    st.markdown("---")

    # Test database connection
    conn_status, conn_message = test_connection()
    if conn_status:
        st.success(f"✅ Database: {conn_message}")
    else:
        st.error(f"❌ Database: {conn_message}")
        st.stop()

    # Sidebar filters
    st.sidebar.header("🔍 Filters")

    # Get subcategories
    subcategories = get_subcategories()
    selected_subcat = st.sidebar.selectbox("📋 Select Subcategory", subcategories)

    # Get user units
    user_units_df = get_user_units()
    user_unit_options = ["All"] + user_units_df['userunit_name'].tolist()
    selected_user_unit = st.sidebar.selectbox("🏢 Select User Unit", user_unit_options)

    # Year filter
    years = ["All", "2020", "2021", "2022", "2023", "2024", "2025"]
    selected_year = st.sidebar.selectbox("📅 Select Year", years)

    # Get filter IDs
    subcat_id = None if selected_subcat == "All" else get_subcatid(selected_subcat)
    user_unit_id = None if selected_user_unit == "All" else user_units_df[user_units_df['userunit_name'] == selected_user_unit]['userunit_id'].iloc[0]

    # Main content
    tab1, tab2, tab3 = st.tabs(["📊 Equipment Records", "🔧 Equipment Analytics", "📈 Spare Parts Prediction"])

    with tab1:
        st.header("📊 Equipment Records")

        # Get equipment records
        eqpt_df = get_eqpt_records(subcat_id, user_unit_id, selected_year)

        if not eqpt_df.empty:
            st.dataframe(eqpt_df, use_container_width=True)

            # Equipment selection for details
            selected_eqpt = st.selectbox("Select Equipment for Details:", eqpt_df['regnno'].tolist())

            if selected_eqpt:
                reg_no, nomenclature, history_df = get_equipment_details(selected_eqpt, selected_year if selected_year != "All" else None)

                st.subheader(f"Equipment Details: {reg_no}")
                st.write(f"**Nomenclature:** {nomenclature}")

                if not history_df.empty:
                    st.dataframe(history_df, use_container_width=True)
                else:
                    st.info("No maintenance history found for this equipment.")
        else:
            st.info("No equipment records found for the selected filters.")

    with tab2:
        st.header("🔧 Equipment Analytics")
        if not eqpt_df.empty:
            # Generate analytics
            analytics_result = generate_equipment_analytics(eqpt_df)
            st.write(analytics_result)
        else:
            st.info("No data available for analytics.")

    with tab3:
        st.header("📈 Spare Parts Prediction")
        if not eqpt_df.empty:
            selected_eqpt_id = st.selectbox("Select Equipment ID:", eqpt_df['eqptid'].tolist())

            if selected_eqpt_id:
                spare_parts_df = get_spare_parts_data(selected_eqpt_id, selected_year if selected_year != "All" else None)

                if not spare_parts_df.empty:
                    prediction_result = generate_spare_parts_prediction(spare_parts_df)
                    st.write(prediction_result)
                else:
                    st.info("No spare parts data available for prediction.")
        else:
            st.info("No equipment data available.")

if __name__ == "__main__":
    main()
'''

# Save the updated application
with open('streamlit_sqlite_app.py', 'w') as f:
    f.write(updated_code)

print("✅ Created 'streamlit_sqlite_app.py' - Updated Streamlit app for SQLite")
print("\nKey changes made:")
print("1. Replaced psycopg2 with sqlite3")
print("2. Updated connection function for SQLite")
print("3. Changed SQL parameter syntax from %s to ?")
print("4. Updated date extraction for SQLite compatibility")
print("5. Removed PostgreSQL-specific functions")
print("6. Added database connection testing")