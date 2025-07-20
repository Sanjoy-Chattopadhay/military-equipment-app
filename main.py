from equipment_analytics import generate_equipment_analytics
import plotly.express as px
import math
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from journey_recommendations import generate_spare_parts_prediction

# --- PostgreSQL/Neon DB Connection ---
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ZgkW9VU8dBcY@ep-holy-cell-a7vl9851-pooler.ap-southeast-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# --- Initialize theme
if "theme" not in st.session_state:
    st.session_state.theme = "light"

# --- Toggle button
if st.button("🌗 Toggle Theme"):
    st.session_state.theme = "dark" if st.session_state.theme == "light" else "light"

# --- Apply CSS for theme
if st.session_state.theme == "dark":
    css = """
        <style>
        html, body, [class*="st-"] {
            background-color: #121212;
            color: #ffffff;
        }
        </style>
    """
else:
    css = """
        <style>
        html, body, [class*="st-"] {
            background-color: #ffffff;
            color: #000000;
        }
        </style>
    """

st.markdown(css, unsafe_allow_html=True)

@st.cache_resource
def get_connection():
    return create_engine(NEON_CONNECTION_STRING)

pd.set_option('display.max_colwidth', None)

# --- FIXED: Get subcategory list ---
def get_subcategories():
    engine = get_connection()
    df = pd.read_sql("SELECT subcategoryname FROM tsubcat WHERE categoryname = 'B'", engine)
    subcats = df["subcategoryname"].dropna().unique().tolist()
    return ["All"] + subcats

# --- FIXED: Get SubCatID from name ---
def get_subcatid(subcategory_name):
    engine = get_connection()
    query = """
            SELECT subcatid 
            FROM tsubcat
            WHERE categoryname = 'B' 
              AND subcategoryname = %(subcategory_name)s
            """
    df = pd.read_sql(query, engine, params={"subcategory_name": subcategory_name})
    return df['subcatid'].iloc[0] if not df.empty else None

# --- FIXED: Get active user units ---
def get_user_units():
    engine = get_connection()
    query = """
            SELECT userunit_id, userunit_name
            FROM tuserunit 
            WHERE movedout = false 
            """
    df = pd.read_sql(query, engine)
    df = df.dropna(subset=["userunit_id", "userunit_name"])
    return [("All", None)] + list(df[["userunit_name", "userunit_id"]].itertuples(index=False, name=None))

# FIXED: Modify the get_eqpt_records function to include year filtering
def get_eqpt_records(subcat_id=None, user_unit_id=None, selected_year=None):
    engine = get_connection()
    query = """
            SELECT r.id AS eqptid, r.regnno, r.nomenclature, r.dtofissue, r.inkm, u.userunit_name
            FROM teqptrecord r
            LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
            LEFT JOIN tsubcat s ON r.cat = s.subcatid
            WHERE s.categoryname = 'B' 
            """
    params = {}

    if subcat_id is not None:
        query += " AND r.cat = %(subcat_id)s"
        params["subcat_id"] = int(subcat_id)

    if user_unit_id is not None:
        query += " AND r.userunit = %(user_unit_id)s"
        params["user_unit_id"] = int(user_unit_id)

    if selected_year and selected_year != "All":
        query += " AND EXTRACT(YEAR FROM r.dtofissue) >= %(selected_year)s"
        params["selected_year"] = int(selected_year)

    query += " ORDER BY r.regnno"

    return pd.read_sql(query, engine, params=params)

# --- FIXED: Fetch total fault count per EqptID ---
def get_fault_counts_per_eqpt():
    query = """
            SELECT r.id AS eqptid
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE f.faults IS NOT NULL 
            """
    df = pd.read_sql(query, get_connection())
    return (
        df.groupby('eqptid')
        .size()
        .reset_index(name='totalfaultcount')
    )

# FIXED: Critical fault counts
def get_critical_fault_counts_per_eqpt(subcat_id=None, user_unit_id=None, selected_year=None):
    try:
        engine = get_connection()
        query = """
            SELECT r.id AS eqptid
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            LEFT JOIN tsubcat s ON r.cat = s.subcatid
            LEFT JOIN tuserunit u ON r.userunit = u.userunit_id
            WHERE jcd.critical = 1 AND s.categoryname = 'B'
        """
        params = {}

        if subcat_id is not None:
            query += " AND r.cat = %(subcat_id)s"
            params["subcat_id"] = int(subcat_id)

        if user_unit_id is not None:
            query += " AND r.userunit = %(user_unit_id)s"
            params["user_unit_id"] = int(user_unit_id)

        if selected_year and selected_year != "All":
            query += " AND EXTRACT(YEAR FROM jc.jobcarddate) >= %(selected_year)s"
            params["selected_year"] = int(selected_year)

        df = pd.read_sql(query, engine, params=params)

        if df.empty:
            return pd.DataFrame(columns=['eqptid', 'totalcriticalfaultcount'])

        return (
            df.groupby('eqptid')
              .size()
              .reset_index(name='totalcriticalfaultcount')
        )

    except Exception as e:
        st.error(f"Error in get_critical_fault_counts_per_eqpt: {e}")
        return pd.DataFrame(columns=['eqptid', 'totalcriticalfaultcount'])

# --- FIXED: Fetch total fault count and fault descriptions per EqptID ---
def get_fault_counts_and_descriptions_per_eqpt():
    query = """
            SELECT r.id AS eqptid, f.faults AS faultdescription
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE f.faults IS NOT NULL 
            """
    df = pd.read_sql(query, get_connection())

    # Group by equipment and aggregate
    result = df.groupby('eqptid').agg(
        totalfaultcount=('faultdescription', 'count'),
        allfaults=('faultdescription', lambda x: ', '.join(sorted(set(x))))
    ).reset_index()

    return result

# --- FIXED: Get detailed fault information for a specific equipment ---
def get_detailed_fault_info(eqpt_id):
    query = """
            SELECT r.id AS eqptid, f.faults AS faultdescription
            FROM teqptrecord r
            LEFT JOIN jobcard jc ON r.id = jc.referid
            LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
            LEFT JOIN tfaults f ON jcd.fault = f.faultid
            WHERE r.id = %(eqpt_id)s
              AND f.faults IS NOT NULL 
            """
    return pd.read_sql(query, get_connection(), params={"eqpt_id": eqpt_id})

def calculate_critical_fault_respect(row):
    count = row.get('totalcriticalfaultcount', 0)
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
    year = row.get('year')
    if pd.isna(year):
        return 'Unknown'
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
    if pd.isna(km):
        return 'Unknown'
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
    - respecttovintage
    - respecttodistance
    - respecttocriticalfaults
    """
    score_map = {
        'Reliable': 3,
        'Partially Reliable': 2,
        'Not Reliable': 1
    }

    cumulative_score = 0
    cumulative_score += score_map.get(row.get('respecttovintage'), 0)
    cumulative_score += score_map.get(row.get('respecttodistance'), 0)
    cumulative_score += score_map.get(row.get('respecttocriticalfaults'), 0)

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
                output_lines.append("")

        if not any_task_shown:
            return f"✅ No scheduled maintenance within the next **{input_km} km**."

        return "\n".join(output_lines)

    except Exception as e:
        return f"⚠️ Error calculating maintenance tasks: {e}"

# FIXED: Modified get_equipment_details() function
def get_equipment_details(regn_no, selected_year=None):
    try:
        engine = get_connection()

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
                LEFT JOIN tsstransactionregister tr ON j.id = tr.refjobid
                LEFT JOIN tssstockmaster sm ON tr.partnoid = sm.id
                WHERE e.regnno = %(regn_no)s
                  AND (tr.issues > 0 OR tr.issues IS NULL)
                """

        params = {"regn_no": regn_no}

        if selected_year and selected_year != "All":
            query += " AND EXTRACT(YEAR FROM j.jobcarddate) >= %(selected_year)s"
            params["selected_year"] = int(selected_year)

        query += " ORDER BY j.jobcarddate DESC"

        df = pd.read_sql(query, engine, params=params)

        if df.empty:
            return None, None, df

        # Get header info
        regn = df['regnno'].iloc[0]
        nomen = df['nomenclature'].iloc[0]

        # DEDUPLICATE DATA: Group by JobCard and aggregate faults/spares
        jobcard_groups = df.groupby(['jobcardno', 'jobcarddate']).agg({
            'faults': lambda x: '; '.join([str(fault) for fault in x.dropna().unique() if str(fault) != 'nan']),
            'itemname': lambda x: '; '.join([str(item) for item in x.dropna().unique() if str(item) != 'nan']),
            'issues': lambda x: x.sum() if x.notna().any() else None
        }).reset_index()

        # Clean up empty aggregations
        jobcard_groups['faults'] = jobcard_groups['faults'].replace('', None)
        jobcard_groups['itemname'] = jobcard_groups['itemname'].replace('', None)

        # Fill None values with appropriate defaults
        jobcard_groups['faults'] = jobcard_groups['faults'].fillna('No faults recorded')
        jobcard_groups['itemname'] = jobcard_groups['itemname'].fillna('No spares used')

        return regn, nomen, jobcard_groups

    except Exception as e:
        st.error(f"Database error: {e}")
        return None, None, pd.DataFrame()

# Keep the rest of your functions the same (modal dialogs, etc.)
@st.dialog("Equipment Fault Details", width="large")
def show_fault_details(eqpt_row):
    # Display RegnNo and Nomenclature as headings
    st.subheader(f"🚗 Registration No: {eqpt_row['regnno']}   📋 Nomenclature: {eqpt_row['nomenclature']}")

    # Create tabs for different views - reordered as requested
    tab1, tab2, tab3, tab4 = st.tabs(["JobCard-History", "Recent Faults", "Maintenance-Forecast", "Fault-History"])

    with tab1:
        # Complete vehicle history using the new function
        with st.spinner("Loading complete history..."):
            regn, nomen, history_df = get_equipment_details(eqpt_row['regnno'], st.session_state.get('year_filter'))

        if history_df is not None and not history_df.empty:
            # Format date
            if 'jobcarddate' in history_df.columns:
                if pd.api.types.is_datetime64_any_dtype(history_df['jobcarddate']):
                    history_df['jobcarddate'] = history_df['jobcarddate'].dt.strftime('%Y-%m-%d')

            # Define columns to show (now using deduplicated data)
            columns_to_show = ['jobcardno', 'jobcarddate', 'faults', 'itemname', 'issues']
            column_config = {
                'jobcardno': st.column_config.TextColumn("Job Card No", width="medium"),
                'jobcarddate': st.column_config.DateColumn("Job Card Date"),
                'faults': st.column_config.TextColumn("Faults (Combined)", width="large"),
                'itemname': st.column_config.TextColumn("Spares (Combined)", width="large"),
                'issues': st.column_config.NumberColumn("Total Issues", format="%d")
            }

            # Display the formatted table
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
                st.metric("Total Job Cards", len(history_df['jobcardno'].dropna().unique()))

            with col2:
                # Count unique faults across all JobCards
                all_faults = []
                for faults_str in history_df['faults'].dropna():
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

    # REPLACEMENT 2: Modified Tab 2 section in show_fault_details()
    with tab2:
        # Recent Faults Analysis from Deduplicated JobCard Data
        st.markdown("### 📊 Recent Faults from JobCard History")

        # Use the deduplicated data from tab1
        with st.spinner("Loading recent faults from JobCard history..."):
            regn, nomen, history_df = get_equipment_details(eqpt_row['regnno'], st.session_state.get('year_filter'))

        if history_df is not None and not history_df.empty:
            # Filter out rows where Faults is null/empty or default message
            filtered_df = history_df[
                history_df['faults'].notna() &
                (history_df['faults'] != '') &
                (history_df['faults'] != 'No faults recorded')
                ]

            if not filtered_df.empty:
                # Sort by JobCardDate to get most recent first
                if 'jobcarddate' in filtered_df.columns:
                    filtered_df = filtered_df.sort_values('jobcarddate', ascending=False)

                # Take only recent JobCards (last 20 JobCards, not fault records)
                recent_jobcards_df = filtered_df.head(20)

                # Now split the concatenated faults and count each fault type
                fault_records = []
                for _, row in recent_jobcards_df.iterrows():
                    faults = str(row['faults']).split('; ')
                    for fault in faults:
                        fault = fault.strip()
                        if fault and fault != 'nan':
                            fault_records.append({
                                'FaultDescription': fault,
                                'JobCardDate': row['jobcarddate'],
                                'SpareName': row['itemname'] if pd.notna(row['itemname']) else 'No spares used'
                            })

                if fault_records:
                    fault_df = pd.DataFrame(fault_records)

                    # Group by fault and aggregate information
                    fault_summary = fault_df.groupby('FaultDescription').agg({
                        'FaultDescription': 'count',  # Count occurrences
                        'SpareName': lambda x: '; '.join(sorted(set(x))),  # Unique spares used
                        'JobCardDate': 'max'  # Most recent occurrence
                    })

                    # Rename columns
                    fault_summary.columns = ['FaultCount', 'SpareName', 'LastOccurrence']
                    fault_summary = fault_summary.reset_index()

                    # Clean up spare names
                    fault_summary['SpareName'] = fault_summary['SpareName'].replace('No spares used', 'No spares used')

                    # Format date column
                    if 'LastOccurrence' in fault_summary.columns:
                        if pd.api.types.is_datetime64_any_dtype(fault_summary['LastOccurrence']):
                            fault_summary['LastOccurrence'] = fault_summary['LastOccurrence'].dt.strftime('%Y-%m-%d')

                    # Sort by last occurrence date descending
                    fault_summary = fault_summary.sort_values('LastOccurrence', ascending=False)

                    # Display fault summary table
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

                    # Summary metrics
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Recent Unique Faults", len(fault_summary))
                    with col2:
                        st.metric("Recent Fault Occurrences", fault_summary['FaultCount'].sum())
                    with col3:
                        most_recent_fault = fault_summary.iloc[0] if not fault_summary.empty else None
                        if most_recent_fault is not None:
                            st.metric("Most Recent Fault Count", f"{most_recent_fault['FaultCount']}x")
                else:
                    st.info("No valid fault records found in recent JobCards.")
            else:
                st.info("No recent fault records found in JobCard history.")
        else:
            st.info("No JobCard history data available for recent faults analysis.")

    with tab3:
        # Maintenance forecast (existing functionality)
        st.markdown("### 🔧 Maintenance Forecast")
        if 'number_input' in st.session_state:
            current_km = eqpt_row.get('inkm', 0)
            input_km = st.session_state['number_input']
            maintenance_msg = get_upcoming_maintenance_tasks(current_km, input_km)
            st.markdown(maintenance_msg)
        else:
            st.warning("Enter km value in the sidebar to view maintenance forecast.")

    # REPLACEMENT 3: Modified Tab 4 section to use consistent data
    with tab4:
        # All-time Fault History from JobCard Data (for consistency)
        st.markdown("### 📊 All-time Fault History from JobCard Data")

        # Use JobCard data but without year filtering to show all-time history
        with st.spinner("Loading all-time fault history..."):
            regn, nomen, all_history_df = get_equipment_details(eqpt_row['regnno'], selected_year=None)

        if all_history_df is not None and not all_history_df.empty:
            # Filter out rows where Faults is null/empty or default message
            filtered_df = all_history_df[
                all_history_df['faults'].notna() &
                (all_history_df['faults'] != '') &
                (all_history_df['faults'] != 'No faults recorded')
                ]

            if not filtered_df.empty:
                # Split concatenated faults and count each fault type
                fault_records = []
                for _, row in filtered_df.iterrows():
                    faults = str(row['faults']).split('; ')
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

    # Close button centered
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        if st.button("Close", key="close_modal", use_container_width=True):
            st.rerun()

def get_filtered_fault_summary(regn_no, selected_year=None):
    try:
        # Use the same function as Tab 2 to get the exact same data
        regn, nomen, history_df = get_equipment_details(regn_no, selected_year)

        if history_df is None or history_df.empty:
            return pd.DataFrame()

        # Filter out rows where Faults is null or empty
        filtered_df = history_df[history_df['faults'].notna() & (history_df['faults'] != '')]

        if filtered_df.empty:
            return pd.DataFrame()

        # Group by fault and count occurrences, then aggregate spares
        fault_summary = filtered_df.groupby('faults').agg({
            'faults': 'count',  # Count fault occurrences
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
        fault_summary.rename(columns={'faults': 'FaultDescription'}, inplace=True)

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
    selected_year = st.selectbox("Select Year", ["All"] + list(range(2000, 2026)), key="year_filter")

    # Integer input
    st.subheader("Destination Distance")
    input_value = st.number_input("Enter a number", min_value=0, step=1, key="number_input")

    # Submit button
    submitted_value = None
    if st.button("Submit", key="submit_btn"):
        submitted_value = input_value
        st.success(f"Destination is {submitted_value} from this base.")

    if st.button("Generate Spare Parts", key="spare_parts_btn"):
        if st.session_state.selected_vehicles:
            generate_spare_parts_prediction(st.session_state.selected_vehicles)
        else:
            st.warning("Select vehicles first!")

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
        # Add calculated columns
        eqpt_df['year'] = pd.to_datetime(eqpt_df['dtofissue'], errors='coerce').dt.year

        # Load Excel InKm data (keeping your existing logic)
        try:
            excel_df = pd.read_excel("RegnInKm.xlsx", usecols=["RegnNo", "InKm"])
            excel_df['InKm'] = pd.to_numeric(excel_df['InKm'], errors='coerce').fillna(0).astype(int)

            # Trim RegnNo to last 5 characters for matching
            excel_df['RegnTail'] = excel_df['RegnNo'].astype(str).str[-5:]
            eqpt_df['RegnTail'] = eqpt_df['regnno'].astype(str).str[-5:]

            # Rename to avoid column clash
            excel_df.rename(columns={'InKm': 'InKm_Excel'}, inplace=True)

            # Merge using trimmed RegnNo
            eqpt_df = eqpt_df.merge(excel_df[['RegnTail', 'InKm_Excel']], on='RegnTail', how='left')

            # Convert DB InKm to numeric
            eqpt_df['inkm'] = pd.to_numeric(eqpt_df['inkm'], errors='coerce').fillna(0).astype(int)

            # Fill missing Excel values with 0
            eqpt_df['InKm_Excel'] = eqpt_df['InKm_Excel'].fillna(0).astype(int)

            # Take maximum of DB and Excel InKm
            eqpt_df['inkm'] = eqpt_df[['inkm', 'InKm_Excel']].max(axis=1)

            # Drop helper columns
            eqpt_df.drop(columns=['InKm_Excel', 'RegnTail'], inplace=True)
        except FileNotFoundError:
            st.warning("RegnInKm.xlsx not found. Using database InKm values only.")
            eqpt_df['inkm'] = pd.to_numeric(eqpt_df['inkm'], errors='coerce').fillna(0).astype(int)
        except Exception as e:
            st.error(f"Error loading Excel data: {e}")
            eqpt_df['inkm'] = pd.to_numeric(eqpt_df['inkm'], errors='coerce').fillna(0).astype(int)

        eqpt_df['respecttovintage'] = eqpt_df.apply(calculate_vintage_respect, axis=1)
        eqpt_df['respecttodistance'] = eqpt_df.apply(calculate_km_respect, axis=1)

        # Merge fault data
        faults_df = get_fault_counts_and_descriptions_per_eqpt()
        eqpt_df = eqpt_df.merge(faults_df, how='left', on='eqptid')
        eqpt_df['totalfaultcount'] = eqpt_df['totalfaultcount'].fillna(0).astype(int)
        eqpt_df['allfaults'] = eqpt_df['allfaults'].fillna('-')

        eqpt_df = eqpt_df.merge(critical_df, how='left', on='eqptid')
        eqpt_df['totalcriticalfaultcount'] = eqpt_df['totalcriticalfaultcount'].fillna(0).astype(int)
        eqpt_df['respecttocriticalfaults'] = eqpt_df.apply(calculate_critical_fault_respect, axis=1)

        # Add Priority column
        eqpt_df['priority'] = eqpt_df.apply(calculate_priority, axis=1)

        # Remove AllFaults from display columns since we'll show it in modal
        columns_to_show = [
            'eqptid', 'regnno', 'nomenclature', 'dtofissue', 'inkm',
            'totalfaultcount', 'totalcriticalfaultcount',
            'respecttovintage', 'respecttodistance', 'respecttocriticalfaults',
            'priority'
        ]

        final_df = eqpt_df[columns_to_show].copy()

        # --- Define custom priority order and sort ---
        priority_order = ['P1', 'P2', 'P3', 'P4', 'P5']
        final_df['priority'] = pd.Categorical(final_df['priority'], categories=priority_order, ordered=True)
        final_df = final_df.sort_values('priority')

        # --- Pagination: Rows per page selector ---
        st.markdown("## Equipment Records")
        rows_per_page = st.selectbox("Rows per page:", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 15], index=9, key="rows_per_page")

        # --- Recalculate pagination based on selection ---
        total_rows = len(final_df)
        total_pages = math.ceil(total_rows / rows_per_page)

        if 'page_number' not in st.session_state or st.session_state.page_number > total_pages:
            st.session_state.page_number = 1  # Reset to page 1 on filter/data change

        st.caption(f"💡 Click on any Registration Number to view detailed fault information")

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

        # Initialize toggle state
        if 'show_analytics' not in st.session_state:
            st.session_state.show_analytics = False

        # Dynamic toggle button
        toggle_label = "Hide Charts & Analytics" if st.session_state.show_analytics else "Show Charts & Analytics"
        if st.button(toggle_label):
            st.session_state.show_analytics = not st.session_state.show_analytics

        # Render the analytics section
        if st.session_state.show_analytics:
            st.markdown("---")
            st.subheader("Charts & Analytics")
            generate_equipment_analytics(eqpt_df, selected_year=st.session_state.get("year_filter"))
            st.markdown("---")

        # Calculate start and end indices for current page
        start_idx = (st.session_state.page_number - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)

        # Display current page data with clickable registration numbers
        current_page_df = final_df.iloc[start_idx:end_idx]

        # Create columns for table headers
        table_cols = st.columns([1, 1.5, 1, 1, 1, 1, 1, 1, 1, 1, 0.5])
        headers = ['Reg No', 'Nomenclature', 'Date of Issue', 'Distance (Km)',
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
                             help="Click to view fault details"):
                    st.session_state.selected_equipment = row
                    show_fault_details(row)

            with row_cols[1]:
                st.write(row['nomenclature'])
            with row_cols[2]:
                st.write(str(row['dtofissue'])[:10] if pd.notna(row['dtofissue']) else '-')
            with row_cols[3]:
                st.write(row['inkm'])
            with row_cols[4]:
                st.write(row['totalfaultcount'])
            with row_cols[5]:
                st.write(row['totalcriticalfaultcount'])
            with row_cols[6]:
                color = "🟢" if row['respecttovintage'] == 'Reliable' else "🟡" if row['respecttovintage'] == 'Partially Reliable' else "🔴"
                st.write(f"{color} {row['respecttovintage']}")
            with row_cols[7]:
                color = "🟢" if row['respecttodistance'] == 'Reliable' else "🟡" if row['respecttodistance'] == 'Partially Reliable' else "🔴"
                st.write(f"{color} {row['respecttodistance']}")
            with row_cols[8]:
                color = "🟢" if row['respecttocriticalfaults'] == 'Reliable' else "🟡" if row['respecttocriticalfaults'] == 'Partially Reliable' else "🔴"
                st.write(f"{color} {row['respecttocriticalfaults']}")
            with row_cols[9]:
                priority_color = {"P1": "🟢", "P2": "🟡", "P3": "🟠", "P4": "🔴", "P5": "⚫"}
                st.write(f"{priority_color.get(row['priority'], '⚪')} {row['priority']}")
            with row_cols[10]:
                # Checkbox for selection
                vehicle_key = f"{row['eqptid']}_{row['regnno']}"
                is_selected = st.checkbox(
                    label="Select vehicle",
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
