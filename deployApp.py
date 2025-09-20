# CHANGE 1: Replace imports and connection setup at the top
# from equipment_analytics import generate_equipment_analytics
import plotly.express as px
import math
import streamlit as st
# import psycopg2  # CHANGED: Replace pyodbc with psycopg2
import pandas as pd
import sqlite3
import os

from equipment_analytics import generate_equipment_analytics
from journey_recommendations import generate_spare_parts_prediction
st.markdown("""
    <style>
    /* Keep header, remove extra top margin */
    header {
        height: 10rem;          /* adjust height if needed */
        min-height: 10rem;
    }

    /* Reduce main content top padding */
    div.block-container {
        padding-top: 2rem !important; /* small padding so content isn’t glued */
    }

    /* Optional: hide footer if needed */
    footer { 
        display: none; 
    }
    </style>
""", unsafe_allow_html=True)



# NeonDB connection details
DB_NAME = "neondb"
DB_USER = "neondb_owner"
DB_PASSWORD = "npg_t70imvFJbTOW"
DB_HOST = "ep-wild-glade-adu0fglb-pooler.c-2.us-east-1.aws.neon.tech"
DB_PORT = "5432"


DB_PATH = r"equipment_database.db"

def get_connection():
    if not os.path.exists(DB_PATH):
        st.error("Database file not found!")
        return None
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def get_subcategories():
    conn = get_connection()
    df = pd.read_sql("SELECT subcategoryname, subcatid FROM tsubcat WHERE categoryname='B'", conn)
    conn.close()
    return ["All"] + df["subcategoryname"].dropna().unique().tolist()

def get_subcatid(subcategory_name):
    conn = get_connection()
    query = """
            SELECT subcatid
            FROM tsubcat
            WHERE categoryname = 'B' 
              AND subcategoryname = ?
            """
    # CHANGED: %s to ? for SQLite
    result = pd.read_sql(query, conn, params=[subcategory_name])
    conn.close()
    return result['subcatid'].iloc[0] if not result.empty else None


def get_user_units():
    conn = get_connection()
    df = pd.read_sql("SELECT userunit_id, userunit_name FROM tuserunit WHERE movedout=0", conn)
    conn.close()
    return [("All", None)] + list(df[["userunit_name", "userunit_id"]].itertuples(index=False, name=None))


def get_eqpt_records(subcat_id=None, user_unit_id=None, selected_year=None):
    conn = get_connection()
    query = """
        SELECT r.id AS eqptid, r.regnno, r.nomenclature, r.dtofissue,
               CAST(r.inkm AS INTEGER) AS inkm, u.userunit_name
        FROM teqptrecord r
        LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
        LEFT JOIN tsubcat s ON r.cat = s.subcatid
        WHERE s.categoryname='B'
    """
    params = []
    if subcat_id: query += " AND r.cat=?"; params.append(int(subcat_id))
    if user_unit_id: query += " AND r.userunit=?"; params.append(int(user_unit_id))
    if selected_year and selected_year != "All":
        query += " AND strftime('%Y', r.dtofissue) >= ?"; params.append(str(selected_year))

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# CHANGE 8: Update get_fault_counts_per_eqpt function
def get_fault_counts_per_eqpt():
    query = """
            SELECT r.id AS eqptid
            FROM teqptrecord AS r
                LEFT JOIN jobcard AS jc ON r.id = jc.referid
                LEFT JOIN jobcarddetails AS jcd ON jc.id = jcd.refjobno
                LEFT JOIN tfaults AS f ON jcd.fault = f.faultid
            WHERE f.faults IS NOT NULL
            """
    # CHANGED: Table and column names to lowercase
    conn = get_connection()
    df = pd.read_sql(query, conn)
    conn.close()  # ADDED: Close connection
    return (
        df.groupby('eqptid')
        .size()
        .reset_index(name='TotalFaultCount')
    )

def get_critical_fault_counts_per_eqpt(subcat_id=None, user_unit_id=None, selected_year=None):
    conn = get_connection()
    query = """
        SELECT r.id AS eqptid
        FROM teqptrecord r
        LEFT JOIN jobcard jc ON r.id = jc.referid
        LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
        LEFT JOIN tfaults f ON jcd.fault = f.faultid
        LEFT JOIN tsubcat s ON r.cat = s.subcatid
        LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
        WHERE jcd.critical=1 AND s.categoryname='B'
    """
    params = []
    if subcat_id: query += " AND r.cat=?"; params.append(int(subcat_id))
    if user_unit_id: query += " AND r.userunit=?"; params.append(int(user_unit_id))
    if selected_year and selected_year != "All":
        query += " AND strftime('%Y', jc.jobcarddate) >= ?"; params.append(str(selected_year))

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    if df.empty: return pd.DataFrame(columns=['eqptid', 'TotalCriticalFaultCount'])
    return df.groupby('eqptid').size().reset_index(name='TotalCriticalFaultCount')

def get_fault_counts_and_descriptions_per_eqpt():
    conn = get_connection()
    query = """
        SELECT r.id AS eqptid, f.faults AS faultdescription
        FROM teqptrecord r
        LEFT JOIN jobcard jc ON r.id = jc.referid
        LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
        LEFT JOIN tfaults f ON jcd.fault = f.faultid
        WHERE f.faults IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()

    return df.groupby('eqptid').agg(
        TotalFaultCount=('faultdescription', 'count'),
        AllFaults=('faultdescription', lambda x: ', '.join(sorted(set(x))))
    ).reset_index()

def get_detailed_fault_info(eqpt_id):
    query = """
            SELECT r.id AS eqptid, f.faults AS faultdescription
            FROM teqptrecord AS r
                LEFT JOIN jobcard AS jc ON r.id = jc.referid
                LEFT JOIN jobcarddetails AS jcd ON jc.id = jcd.refjobno
                LEFT JOIN tfaults AS f ON jcd.fault = f.faultid
            WHERE r.id = ?
              AND f.faults IS NOT NULL
            """
    # CHANGED: %s to ? for SQLite
    conn = get_connection()
    result = pd.read_sql(query, conn, params=[eqpt_id])
    conn.close()
    return result


def calculate_critical_fault_respect(row):
    count = row.get('TotalCriticalFaultCount', 0)
    if pd.isna(count):
        return 'Unknown'
    try:
        count = int(count)
        if count <= 2:
            return 'Reliable'
        elif 3 <= count <= 5:
            return 'Partially Reliable'
        else:
            return 'Not Reliable'
    except:
        return 'Invalid'


# --- Respect logic ---
def calculate_vintage_respect(row):
    year = row.get('Year')
    if pd.isna(year): return 'Unknown'
    try:
        year = int(year)
        if year <= 2009:
            return 'Not Reliable'
        elif year < 2015:
            return 'Partially Reliable'
        return 'Reliable'
    except:
        return 'Invalid'


def calculate_km_respect(row):
    km = row.get('inkm', 0)
    if pd.isna(km): return 'Unknown'
    try:
        if km <= 40000:
            return 'Reliable'
        elif km <= 90000:
            return 'Partially Reliable'
        return 'Not Reliable'
    except:
        return 'Invalid'


# --- Priority Scoring ---
def calculate_priority(row):
    """
    Calculate priority based on cumulative scoring from three reliability factors:
    - RespectToVintage
    - RespectToDistance
    - RespectToCriticalFaults
    """
    score_map = {
        'Reliable': 3,
        'Partially Reliable': 2,
        'Not Reliable': 1
    }

    cumulative_score = 0
    cumulative_score += score_map.get(row.get('RespectToVintage'), 0)
    cumulative_score += score_map.get(row.get('RespectToDistance'), 0)
    cumulative_score += score_map.get(row.get('RespectToCriticalFaults'), 0)

    if cumulative_score == 9:
        return 'P1'
    elif cumulative_score == 8:
        return 'P2'
    elif cumulative_score == 7:
        return 'P3'
    elif cumulative_score == 6:
        return 'P4'
    else:
        return 'P5'


# --- Create Pie Charts ---
def create_pie_chart(data, column, title):
    value_counts = data[column].value_counts()
    fig = px.pie(
        values=value_counts.values,
        names=value_counts.index,
        title=title,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig.update_layout(
        height=400,
        showlegend=True,
        title_x=0.5
    )
    return fig


def get_upcoming_maintenance_tasks(current_km, input_km):
    service_tasks = {
        5000: [
            "Change engine oil and oil filter",
            "Replace fuel filter",
            "Inspect and adjust brakes"
        ],
        10000: [
            "Check gearbox and differential oil",
            "Inspect and adjust clutch",
            "Inspect suspension system"
        ],
        20000: [
            "Engine tune-up",
            "Clean fuel tank and lines"
        ]
    }

    if pd.isna(current_km) or pd.isna(input_km):
        return "🚫 Insufficient data to calculate upcoming maintenance."

    try:
        current_km = int(current_km)
        input_km = int(input_km)
        future_km = current_km + input_km

        output_lines = [f"### 🔧 Maintenance due within next **{input_km} km**"]

        any_task_shown = False

        for interval, tasks in service_tasks.items():
            next_due_km = ((current_km // interval) + 1) * interval
            km_remaining = next_due_km - current_km

            if next_due_km <= future_km:
                any_task_shown = True
                output_lines.append(f"**After {km_remaining} km (at {next_due_km:,} km)** → Perform:")
                for task in tasks:
                    output_lines.append(f"- {task} (Every {interval:,} km)")
                output_lines.append("")  # spacing

        if not any_task_shown:
            return f"✅ No scheduled maintenance within the next **{input_km} km**."

        return "\n".join(output_lines)

    except Exception as e:
        return f"⚠️ Error calculating maintenance tasks: {e}"

def get_equipment_details(regn_no, selected_year=None):
    """Fetch equipment jobcard history and aggregate faults and spares safely"""
    regn, nomen = None, None
    jobcard_groups = pd.DataFrame()
    try:
        conn = get_connection()

        query = """
            SELECT e.regnno,
                   e.nomenclature,
                   j.jobcardno,
                   j.jobcarddate,
                   f.faults,
                   sm.itemname,
                   tr.issues
            FROM teqptrecord e
                LEFT JOIN jobcard j ON e.id = j.referid
                LEFT JOIN jobcarddetails jd ON j.id = jd.refjobno
                LEFT JOIN tfaults f ON jd.fault = f.faultid
                LEFT JOIN tsstransactionregister tr ON j.id = CAST(tr.refjobid AS BIGINT)
                LEFT JOIN tssstockmaster sm ON tr.partnoid = sm.id
            WHERE e.regnno = ?
        """
        params = [regn_no]

        query += " ORDER BY j.jobcarddate DESC"
        df = pd.read_sql(query, conn, params=params)
        conn.close()

        if df.empty:
            return None, None, pd.DataFrame()

        regn = df['regnno'].iloc[0]
        nomen = df['nomenclature'].iloc[0]

        # Safely convert jobcarddate to datetime, invalid parsing becomes NaT
        df['jobcarddate'] = pd.to_datetime(df['jobcarddate'], errors='coerce')

        # Filter by year if requested
        if selected_year and selected_year != "All":
            df = df[df['jobcarddate'].dt.year >= int(selected_year)]

        # Aggregate faults and spares per jobcard safely
        jobcard_groups = df.groupby(['jobcardno', 'jobcarddate']).agg({
            'faults': lambda x: '; '.join([str(f) for f in x.dropna().unique() if str(f) != 'nan']),
            'itemname': lambda x: '; '.join([str(i) for i in x.dropna().unique() if str(i) != 'nan']),
            'issues': lambda x: x.sum() if x.notna().any() else 0
        }).reset_index()

        # Replace empty values with defaults
        jobcard_groups['faults'] = jobcard_groups['faults'].replace('', None).fillna('No faults recorded')
        jobcard_groups['itemname'] = jobcard_groups['itemname'].replace('', None).fillna('No spares used')

        # Rename columns
        jobcard_groups = jobcard_groups.rename(columns={
            'jobcardno': 'JobCardNo',
            'jobcarddate': 'JobCardDate',
            'faults': 'Faults',
            'itemname': 'ItemName',
            'issues': 'Issues'
        })

    except Exception as e:
        st.error(f"Error fetching equipment details: {e}")
        return None, None, pd.DataFrame()

    return regn, nomen, jobcard_groups




# ----------- Inside Streamlit Modal (tab2) -----------
    with tab2:
        st.markdown("### 📊 Recent Faults from JobCard History")

        with st.spinner("Loading recent faults from JobCard history..."):
            regn, nomen, history_df = get_equipment_details(eqpt_row['regnno'], st.session_state.get('year_filter'))

        if history_df is not None and not history_df.empty:
            # Filter out rows with no valid faults
            filtered_df = history_df[
                history_df['Faults'].notna() &
                (history_df['Faults'] != '') &
                (history_df['Faults'] != 'No faults recorded')
            ]

            if not filtered_df.empty:
                # Sort by JobCardDate descending
                filtered_df = filtered_df.sort_values('JobCardDate', ascending=False)
                recent_jobcards_df = filtered_df.head(20)

                fault_records = []
                for _, row in recent_jobcards_df.iterrows():
                    faults = str(row['Faults']).split('; ')
                    for fault in faults:
                        fault = fault.strip()
                        if fault and fault != 'nan':
                            fault_records.append({
                                'FaultDescription': fault,
                                'JobCardDate': row['JobCardDate'],
                                'SpareName': row['ItemName'] if pd.notna(row['ItemName']) else 'No spares used'
                            })

                if fault_records:
                    fault_df = pd.DataFrame(fault_records)

                    # Aggregate faults
                    fault_summary = fault_df.groupby('FaultDescription').agg({
                        'FaultDescription': 'count',
                        'SpareName': lambda x: '; '.join(sorted(set(x))),
                        'JobCardDate': 'max'
                    }).rename(columns={'FaultDescription': 'FaultCount', 'SpareName': 'SpareName', 'JobCardDate': 'LastOccurrence'}).reset_index()

                    # Format LastOccurrence date
                    if pd.api.types.is_datetime64_any_dtype(fault_summary['LastOccurrence']):
                        fault_summary['LastOccurrence'] = fault_summary['LastOccurrence'].dt.strftime('%Y-%m-%d')

                    fault_summary = fault_summary.sort_values('LastOccurrence', ascending=False)

                    st.dataframe(
                        fault_summary,
                        use_container_width=True,
                        hide_index=True,
                        height=min(400, len(fault_summary) * 35 + 50),
                        column_config={
                            "FaultDescription": st.column_config.TextColumn("Fault Description", width="large"),
                            "FaultCount": st.column_config.NumberColumn("Count", width="small", format="%d"),
                            "SpareName": st.column_config.TextColumn("Spares Used", width="large"),
                            "LastOccurrence": st.column_config.DateColumn("Last Occurrence", width="medium")
                        }
                    )

                    col1, col2, col3 = st.columns(3)
                    col1.metric("Recent Unique Faults", len(fault_summary))
                    col2.metric("Recent Fault Occurrences", fault_summary['FaultCount'].sum())
                    most_recent_fault = fault_summary.iloc[0] if not fault_summary.empty else None
                    if most_recent_fault is not None:
                        col3.metric("Most Recent Fault Count", f"{most_recent_fault['FaultCount']}x")
                else:
                    st.info("No valid fault records found in recent JobCards.")
            else:
                st.info("No recent fault records found in JobCard history.")
        else:
            st.info("No JobCard history data available for recent faults analysis.")


    # REPLACEMENT 3: Modified Tab 4 section to use consistent data
    # Replace the entire "with tab4:" section with this:

    with tab4:
        # All-time Fault History from JobCard Data (for consistency)
        st.markdown("### 📊 All-time Fault History from JobCard Data")

        # Use JobCard data but without year filtering to show all-time history
        with st.spinner("Loading all-time fault history..."):
            regn, nomen, all_history_df = get_equipment_details(eqpt_row['regnno'], selected_year=None)

        if all_history_df is not None and not all_history_df.empty:
            # Filter out rows where Faults is null/empty or default message
            filtered_df = all_history_df[
                all_history_df['Faults'].notna() &
                (all_history_df['Faults'] != '') &
                (all_history_df['Faults'] != 'No faults recorded')
                ]

            if not filtered_df.empty:
                # Split concatenated faults and count each fault type
                fault_records = []
                for _, row in filtered_df.iterrows():
                    faults = str(row['Faults']).split('; ')
                    for fault in faults:
                        fault = fault.strip()
                        if fault and fault != 'nan':
                            fault_records.append(fault)

                if fault_records:
                    # Count fault occurrences
                    fault_counts = pd.Series(fault_records).value_counts().reset_index()
                    fault_counts.columns = ['Fault Description', 'Count']
                    fault_counts = fault_counts.sort_values(['Count', 'Fault Description'], ascending=[False, True])

                    st.dataframe(
                        fault_counts,
                        use_container_width=True,
                        hide_index=True,
                        height=min(400, len(fault_counts) * 35 + 50),
                        column_config={
                            "Fault Description": st.column_config.TextColumn("Fault Description", width="large"),
                            "Count": st.column_config.NumberColumn("Count", width="small", format="%d")
                        }
                    )

                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Unique Faults", len(fault_counts))
                    with col2:
                        st.metric("Total Occurrences", len(fault_records))
                    with col3:
                        st.metric("Most Frequent", f"{fault_counts.iloc[0]['Count']}x")
                else:
                    st.info("No valid fault records found.")
            else:
                st.info("No fault records found in JobCard history.")
        else:
            st.info("No JobCard history data available.")

    with tab1:
        # Complete vehicle history using the modified function
        with st.spinner("Loading complete history..."):
            regn, nomen, history_df = get_equipment_details(eqpt_row['regnno'], st.session_state.get('year_filter'))

        if history_df is not None and not history_df.empty:
            # Format date
            if 'JobCardDate' in history_df.columns:
                if pd.api.types.is_datetime64_any_dtype(history_df['JobCardDate']):
                    history_df['JobCardDate'] = history_df['JobCardDate'].dt.strftime('%Y-%m-%d')

            # Define columns to show (now using deduplicated data)
            # Correct column names after renaming in get_equipment_details()
            columns_to_show = ['JobCardNo', 'JobCardDate', 'Faults', 'ItemName', 'Issues']

            column_config = {
                'JobCardNo': st.column_config.TextColumn("Job Card No", width="medium"),
                'JobCardDate': st.column_config.DateColumn("Job Card Date"),
                'Faults': st.column_config.TextColumn("Fault Name", width="large"),
                'ItemName': st.column_config.TextColumn("Spare Name", width="large"),
                'Issues': st.column_config.NumberColumn("Total Issues", format="%d")
            }

            # When displaying the table
            st.dataframe(
                history_df[columns_to_show],
                use_container_width=True,
                hide_index=True,
                height=400,
                column_config=column_config
            )

            # Show summary metrics (now accurate)
            st.subheader("Summary")
            col1, col2, col3 = st.columns(3)

            with col1:
                st.metric("Total Job Cards", len(history_df['JobCardNo'].dropna().unique()))

            with col2:
                # Count unique faults across all JobCards
                all_faults = []
                for faults_str in history_df['Faults'].dropna():
                    if faults_str != 'No faults recorded':
                        faults = [f.strip() for f in str(faults_str).split(';') if f.strip()]
                        all_faults.extend(faults)
                unique_faults = len(set(all_faults)) if all_faults else 0
                st.metric("Unique Faults", unique_faults)

            with col3:
                # Count unique spares across all JobCards
                all_spares = []
                for spares_str in history_df['itemname'].dropna():
                    if spares_str != 'No spares used':
                        spares = [s.strip() for s in str(spares_str).split(';') if s.strip()]
                        all_spares.extend(spares)
                unique_spares = len(set(all_spares)) if all_spares else 0
                st.metric("Unique Spares", unique_spares)

        else:
            st.warning("No detailed history found for this registration number")


@st.dialog("Equipment Fault Details", width="large")
def show_fault_details(eqpt_row):
    st.subheader(f"🚗 Registration No: {eqpt_row['regnno']}   📋 Nomenclature: {eqpt_row.get('nomenclature', 'N/A')}")

    # Create tabs
    tab1, tab2, tab3, tab4 = st.tabs(["JobCard-History", "Recent Faults", "Maintenance-Forecast", "Fault-History"])

    # Fetch jobcard history
    regn, nomen, history_df = get_equipment_details(eqpt_row['regnno'], st.session_state.get('year_filter'))

    if history_df is None or history_df.empty:
        st.warning("No JobCard history data available.")
        return

    # Ensure all expected columns exist and fill defaults
    expected_columns = {
        'JobCardNo': 'N/A',
        'JobCardDate': 'N/A',
        'Faults': 'No faults recorded',
        'ItemName': 'No spares used',
        'Issues': 0
    }
    for col, default in expected_columns.items():
        if col not in history_df.columns:
            history_df[col] = default

    # ---------------- Tab 1: Complete JobCard History ----------------
    with tab1:
        st.markdown("### 📝 JobCard History")
        st.dataframe(
            history_df[list(expected_columns.keys())],
            use_container_width=True,
            hide_index=True,
            height=400
        )

        st.subheader("Summary")
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Job Cards", history_df['JobCardNo'].nunique())
        col2.metric("Unique Faults", history_df['Faults'].nunique())
        col3.metric("Unique Spares", history_df['ItemName'].nunique())

    # ---------------- Tab 2: Recent Faults ----------------
    with tab2:
        st.markdown("### 📊 Recent Faults")
        recent_df = history_df[history_df['Faults'].notna() & (history_df['Faults'] != 'No faults recorded')]
        if not recent_df.empty:
            recent_faults = []
            for _, row in recent_df.iterrows():
                faults = str(row['Faults']).split(';')
                for fault in faults:
                    fault = fault.strip()
                    if fault:
                        recent_faults.append({
                            'FaultDescription': fault,
                            'JobCardDate': row['JobCardDate'],
                            'SpareName': row['ItemName']
                        })
            if recent_faults:
                fault_summary = pd.DataFrame(recent_faults).groupby('FaultDescription').agg({
                    'FaultDescription': 'count',
                    'SpareName': lambda x: ', '.join(sorted(set(x))),
                    'JobCardDate': 'max'
                }).rename(columns={'FaultDescription': 'FaultCount', 'SpareName': 'SpareName', 'JobCardDate': 'LastOccurrence'}).reset_index()
                st.dataframe(fault_summary, use_container_width=True, hide_index=True)
            else:
                st.info("No recent faults found.")
        else:
            st.info("No recent faults found.")

    # ---------------- Tab 3: Maintenance Forecast ----------------
    with tab3:
        st.markdown("### 🔧 Maintenance Forecast")
        if 'number_input' in st.session_state:
            current_km = eqpt_row.get('inkm', 0)
            input_km = st.session_state['number_input']
            st.markdown(get_upcoming_maintenance_tasks(current_km, input_km))
        else:
            st.warning("Enter km value in the sidebar to view maintenance forecast.")

    # ---------------- Tab 4: All-time Fault History ----------------
    with tab4:
        st.markdown("### 📊 All-time Fault History")
        fault_list = []
        for _, row in history_df.iterrows():
            faults = str(row['Faults']).split(';')
            for fault in faults:
                fault = fault.strip()
                if fault:
                    fault_list.append(fault)
        if fault_list:
            fault_counts = pd.Series(fault_list).value_counts().reset_index()
            fault_counts.columns = ['FaultDescription', 'Count']
            st.dataframe(fault_counts, use_container_width=True, hide_index=True)
        else:
            st.info("No fault records found.")

    # ---------------- Close Button ----------------
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Close", key="close_modal"):
            st.rerun()



def get_filtered_fault_summary(regn_no, selected_year=None):
    try:
        # Use the same function as Tab 2 to get the exact same data
        regn, nomen, history_df = get_equipment_details(regn_no, selected_year)

        if history_df is None or history_df.empty:
            return pd.DataFrame()

        # Filter out rows where Faults is null or empty
        filtered_df = history_df[history_df['Faults'].notna() & (history_df['Faults'] != '')]

        if filtered_df.empty:
            return pd.DataFrame()

        # Group by fault and count occurrences, then aggregate spares
        fault_summary = filtered_df.groupby('Faults').agg({
            'Faults': 'count',  # Count fault occurrences
            'itemname': lambda x: ', '.join(sorted(set(x.dropna().astype(str))))  # Aggregate unique spares
        })

        # Flatten column names
        fault_summary.columns = ['FaultCount', 'SpareName']

        # Clean up spare names - handle empty/null values
        fault_summary['SpareName'] = fault_summary['SpareName'].replace('', 'No spares used')
        fault_summary['SpareName'] = fault_summary['SpareName'].replace('nan', 'No spares used')
        fault_summary['SpareName'] = fault_summary['SpareName'].str.replace('nan, ', '')
        fault_summary['SpareName'] = fault_summary['SpareName'].str.replace(', nan', '')

        # Reset index to make fault description a column
        fault_summary = fault_summary.reset_index()
        fault_summary.rename(columns={'Faults': 'FaultDescription'}, inplace=True)

        # Sort by fault count descending
        fault_summary = fault_summary.sort_values('FaultCount', ascending=False)

        return fault_summary

    except Exception as e:
        st.error(f"Error getting fault summary: {e}")
        return pd.DataFrame()


# --- UI Starts ---
st.set_page_config(page_title="Equipment Viewer", layout="wide")



# Initialize session state for modal
if 'show_modal' not in st.session_state:
    st.session_state.show_modal = False
if 'selected_equipment' not in st.session_state:
    st.session_state.selected_equipment = None


# Initialize session state for checkboxes
if 'selected_vehicles' not in st.session_state:
    st.session_state.selected_vehicles = []

# Create main layout with sidebar and main content
col1, col2 = st.columns([1, 4])  # 25% and 75% split

# --- Sidebar Controls (Left 25%) ---
with col1:
    eqpt_df = get_eqpt_records()
    # Subcategory selection
    st.subheader("Vehicle Name")
    subcategories = get_subcategories()
    selected_subcategory = st.selectbox("Choose Subcategory", subcategories, key="subcat")

    # User Unit selection
    st.subheader("User Unit")
    user_unit_options = get_user_units()
    selected_user_unit_name = st.selectbox("Choose User Unit", [name for name, _ in user_unit_options], key="userunit")
    selected_user_unit_id = next((_id for name, _id in user_unit_options if name == selected_user_unit_name), None)

    # Year filtering
    st.subheader("Year Filter")
    st.caption(f"💡 The filter gives faults occured in the vehicles after the selected year till date")

    selected_year = st.selectbox("Select Year", ["All"] + list(range(2000, 2026)), key="year_filter")

    # Integer input
    st.subheader("Destination Distance")
    input_value = st.number_input("Enter a number", min_value=0, step=1, key="number_input")

    # Submit button
    submitted_value = None
    if st.button("Submit", key="submit_btn"):
        submitted_value = input_value
        st.success(f"Destination is {submitted_value} from this base.")


with col2:
    if selected_subcategory == "All":
        eqpt_df = get_eqpt_records(user_unit_id=selected_user_unit_id, selected_year=selected_year)
        critical_df = get_critical_fault_counts_per_eqpt(user_unit_id=selected_user_unit_id,
                                                         selected_year=selected_year)
    else:
        subcat_id = get_subcatid(selected_subcategory)
        eqpt_df = get_eqpt_records(subcat_id=subcat_id, user_unit_id=selected_user_unit_id,
                                   selected_year=selected_year) if subcat_id else pd.DataFrame()
        critical_df = get_critical_fault_counts_per_eqpt(subcat_id=subcat_id, user_unit_id=selected_user_unit_id,
                                                         selected_year=selected_year) if subcat_id else pd.DataFrame()

    # Process data if available
    if not eqpt_df.empty:

        eqpt_df['Year'] = pd.to_datetime(eqpt_df['dtofissue'], errors='coerce').dt.year
        eqpt_df['inkm'] = pd.to_numeric(eqpt_df['inkm'], errors='coerce').fillna(0).astype(int)
        eqpt_df['RespectToVintage'] = eqpt_df.apply(calculate_vintage_respect, axis=1)
        eqpt_df['RespectToDistance'] = eqpt_df.apply(calculate_km_respect, axis=1)

        # Merge fault data (count + all descriptions)
        faults_df = get_fault_counts_and_descriptions_per_eqpt()
        eqpt_df = eqpt_df.merge(faults_df, how='left', on='eqptid')
        eqpt_df['TotalFaultCount'] = eqpt_df['TotalFaultCount'].fillna(0).astype(int)
        eqpt_df['AllFaults'] = eqpt_df['AllFaults'].fillna('-')

        # Merge critical fault data
        eqpt_df = eqpt_df.merge(critical_df, how='left', on='eqptid')
        eqpt_df['TotalCriticalFaultCount'] = eqpt_df['TotalCriticalFaultCount'].fillna(0).astype(int)
        eqpt_df['RespectToCriticalFaults'] = eqpt_df.apply(calculate_critical_fault_respect, axis=1)

        # Add Priority column
        eqpt_df['Priority'] = eqpt_df.apply(calculate_priority, axis=1)

        # Remove AllFaults from display columns since we'll show it in modal
        # Change column names in columns_to_show:
        columns_to_show = [
            'eqptid', 'regnno', 'nomenclature', 'dtofissue', 'inkm',  # CHANGED: All to lowercase
            'TotalFaultCount', 'TotalCriticalFaultCount',
            'RespectToVintage', 'RespectToDistance', 'RespectToCriticalFaults',
            'Priority'
        ]


        final_df = eqpt_df[columns_to_show].copy()

        # --- Define custom priority order and sort ---
        priority_order = ['P1', 'P2', 'P3', 'P4', 'P5']
        final_df['Priority'] = pd.Categorical(final_df['Priority'], categories=priority_order, ordered=True)
        final_df = final_df.sort_values('Priority')

        # --- Pagination: Rows per page selector ---
        st.markdown("## Equipment Records")
        rows_per_page = st.selectbox("Rows per page:", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15], index=9, key="rows_per_page")

        # --- Recalculate pagination based on selection ---
        total_rows = len(final_df)
        total_pages = math.ceil(total_rows / rows_per_page)

        if 'page_number' not in st.session_state or st.session_state.page_number > total_pages:
            st.session_state.page_number = 1  # Reset to page 1 on filter/data change

        # st.caption(f"💡 Click on any Registration Number to view detailed fault information")

        # --- Pagination Controls ---
        pagination_container = st.container()
        with pagination_container:
            col1, col2, col3, col4 = st.columns([1, 2, 2, 1])

            with col1:
                prev_disabled = st.session_state.page_number <= 1
                if st.button("Previous", disabled=prev_disabled):
                    st.session_state.page_number -= 1
                    st.rerun()

            with col2:
                st.markdown(
                    f"<div style='padding-top:8px;'>Page {st.session_state.page_number} of {total_pages}</div>",
                    unsafe_allow_html=True
                )

            with col3:
                if total_pages > 1:
                    selected_page = st.selectbox(
                        "Jump to page:",
                        options=list(range(1, total_pages + 1)),
                        index=st.session_state.page_number - 1,
                        label_visibility="collapsed"
                    )
                    if selected_page != st.session_state.page_number:
                        st.session_state.page_number = selected_page
                        st.rerun()
                else:
                    st.markdown("<div style='padding-top:8px;'>Only 1 page</div>", unsafe_allow_html=True)

            with col4:
                next_disabled = st.session_state.page_number >= total_pages
                if st.button("Next", disabled=next_disabled):
                    st.session_state.page_number += 1
                    st.rerun()

        # --- Show only the current page data ---
        start_idx = (st.session_state.page_number - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)
        current_page_df = final_df.iloc[start_idx:end_idx]

        # # Analytics button placed at top of table
        # st.markdown("###")
        # Initialize toggle state
        if 'show_analytics' not in st.session_state:
            st.session_state.show_analytics = False

        # Toggle button
        if st.button("📊 Generate Charts & Analytics", key="analytics_btn"):
            st.session_state.show_analytics = not st.session_state.show_analytics

        # Show or hide analytics based on toggle state
        if st.session_state.show_analytics:
            generate_equipment_analytics(eqpt_df, selected_year=st.session_state.get("year_filter"))

        # Dynamic toggle button
        # toggle_label = "Hide Charts & Analytics" if st.session_state.show_analytics else "Show Charts & Analytics"
        # if st.button(toggle_label):
        #     st.session_state.show_analytics = not st.session_state.show_analytics

        # Render the analytics section
        if st.session_state.show_analytics:
            st.markdown("---")
            st.subheader("Charts & Analytics")
            # generate_equipment_analytics(eqpt_df, selected_year=st.session_state.get("year_filter"))
            st.markdown("---")

        # Calculate start and end indices for current page
        start_idx = (st.session_state.page_number - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)

        # Display current page data with clickable registration numbers
        current_page_df = final_df.iloc[start_idx:end_idx]


        # Create columns for table headers
        table_cols = st.columns([1, 1.5, 1, 1, 1, 1, 1, 1, 1, 1, 0.5])
        headers = ['Reg No', 'nomenclature', 'Date of Issue', 'Distance (Km)',
                   'Total Faults', 'Critical Faults', 'Vintage', 'Distance', 'Critical', 'Priority', 'Check']

        for i, header in enumerate(headers):
            with table_cols[i]:
                st.write(f"**{header}**")

        st.divider()

        # Display rows with clickable registration numbers
        for idx, row in current_page_df.iterrows():
            row_cols = st.columns([1, 1.5, 1, 1, 1, 1, 1, 1, 1, 1, 0.5])

            with row_cols[0]:
                # Make registration number clickable
                if st.button(row['regnno'], key=f"reg_{row['eqptid']}_{idx}",
                             # CHANGED: RegnNo to regnno, EqptID to eqptid
                             help="Click to view fault details"):
                    st.session_state.selected_equipment = row
                    show_fault_details(row)

            with row_cols[1]:
                st.write(row['nomenclature'])  # CHANGED: nomenclature to nomenclature
            with row_cols[2]:
                st.write(str(row['dtofissue'])[:10] if pd.notna(
                    row['dtofissue']) else '-')  # CHANGED: DtOfIssue to dtofissue
            with row_cols[3]:
                st.write(row['inkm'])  # CHANGED: inkm to inkm
            with row_cols[4]:
                st.write(row['TotalFaultCount'])
            with row_cols[5]:
                st.write(row['TotalCriticalFaultCount'])
            with row_cols[6]:
                color = "🟢" if row['RespectToVintage'] == 'Reliable' else "🟡" if row[
                                                                                     'RespectToVintage'] == 'Partially Reliable' else "🔴"
                st.write(f"{color} {row['RespectToVintage']}")
            with row_cols[7]:
                color = "🟢" if row['RespectToDistance'] == 'Reliable' else "🟡" if row[
                                                                                      'RespectToDistance'] == 'Partially Reliable' else "🔴"
                st.write(f"{color} {row['RespectToDistance']}")
            with row_cols[8]:
                color = "🟢" if row['RespectToCriticalFaults'] == 'Reliable' else "🟡" if row[
                                                                                            'RespectToCriticalFaults'] == 'Partially Reliable' else "🔴"
                st.write(f"{color} {row['RespectToCriticalFaults']}")
            with row_cols[9]:
                priority_color = {"P1": "🟢", "P2": "🟡", "P3": "🟠", "P4": "🔴", "P5": "⚫"}
                st.write(f"{priority_color.get(row['Priority'], '⚪')} {row['Priority']}")
            with row_cols[10]:
                # Checkbox for selection
                vehicle_key = f"{row['eqptid']}_{row['regnno']}"
                is_selected = st.checkbox(
                    label="Select vehicle",  # Still required internally
                    key=f"check_{vehicle_key}",
                    value=vehicle_key in st.session_state.selected_vehicles,
                    label_visibility="collapsed"
                )

                if is_selected and vehicle_key not in st.session_state.selected_vehicles:
                    st.session_state.selected_vehicles.append(vehicle_key)
                elif not is_selected and vehicle_key in st.session_state.selected_vehicles:
                    st.session_state.selected_vehicles.remove(vehicle_key)

    else:
        st.info("No equipment records found for the selected filters.")