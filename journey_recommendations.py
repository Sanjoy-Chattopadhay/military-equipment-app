import streamlit as st
import pandas as pd
from datetime import datetime
import plotly.express as px
from io import BytesIO


def get_vehicle_data_by_key(vehicle_key):
    eqpt_id, regn_no = vehicle_key.split('_', 1)
    return eqpt_id, regn_no


def get_recent_jobcard_data(regn_no):
    from __main__ import get_equipment_details

    selected_year = st.session_state.get('year_filter', None)

    if selected_year == "All":
        selected_year = None
    elif selected_year is not None:
        try:
            selected_year = int(selected_year)
        except:
            selected_year = None

    regn, nomen, history_df = get_equipment_details(regn_no, selected_year=selected_year)
    return regn, nomen, history_df


def get_current_odometer_reading(regn_no):
    """Get current odometer reading for a vehicle.
    You may need to implement this based on your data source."""
    # This is a placeholder - implement based on your data source
    # For example, you might get this from your main vehicle data
    try:
        from __main__ import get_current_km  # Adjust based on your actual function
        return get_current_km(regn_no)
    except:
        # Default fallback - you might want to get this from session state or database
        return st.session_state.get(f'current_km_{regn_no}', 50000)  # Default to 50000


def download_excel_button(df, filename="jobcard_history.xlsx"):
    """Create an Excel download button for a given dataframe."""
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='History')
    st.download_button(
        label="📥 Download Excel",
        data=output.getvalue(),
        file_name=filename,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def generate_vehicle_summary(history_df, regn):
    """Generate comprehensive summary statistics for a vehicle's history."""
    if history_df is None or history_df.empty:
        return None

    summary_data = {}

    # Basic counts
    summary_data['Total Job Cards'] = len(history_df)

    # Date range analysis
    if 'JobCardDate' in history_df.columns:
        try:
            history_df['JobCardDate'] = pd.to_datetime(history_df['JobCardDate'], errors='coerce')
            date_range = history_df['JobCardDate'].dropna()
            if not date_range.empty:
                summary_data[
                    'Date Range'] = f"{date_range.min().strftime('%Y-%m-%d')} to {date_range.max().strftime('%Y-%m-%d')}"
                summary_data['Days Covered'] = (date_range.max() - date_range.min()).days
        except:
            pass

    # Spare parts analysis
    if 'itemname' in history_df.columns:
        spares_series = history_df['itemname'].dropna().str.split(';').explode().str.strip()
        spares_series = spares_series[spares_series != '']  # Remove empty strings

        summary_data['Unique Spare Parts'] = len(spares_series.unique())
        summary_data['Total Spare Parts Used'] = len(spares_series)

        if len(spares_series) > 0:
            top_spare = spares_series.value_counts().head(1)
            summary_data['Most Used Spare Part'] = f"{top_spare.index[0]} ({top_spare.iloc[0]}x)"

    # Quantity analysis
    if 'QuantityRequired' in history_df.columns:
        quantities = pd.to_numeric(history_df['QuantityRequired'], errors='coerce').dropna()
        if not quantities.empty:
            summary_data['Total Quantity Required'] = int(quantities.sum())
            summary_data['Avg Quantity per Job'] = round(quantities.mean(), 2)

    # Fault analysis
    if 'Faults' in history_df.columns:
        fault_series = history_df['Faults'].dropna().str.split(';').explode().str.strip()
        fault_series = fault_series[fault_series != '']  # Remove empty strings

        summary_data['Unique Fault Types'] = len(fault_series.unique())
        summary_data['Total Fault Occurrences'] = len(fault_series)

        if len(fault_series) > 0:
            top_fault = fault_series.value_counts().head(1)
            summary_data['Most Common Fault'] = f"{top_fault.index[0]} ({top_fault.iloc[0]}x)"

    return summary_data


def display_summary_metrics(summary_data, regn):
    """Display summary metrics in a nice format."""
    if not summary_data:
        return

    st.markdown(f"#### 📊 Summary Statistics for {regn}")

    # Create columns for metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if 'Total Job Cards' in summary_data:
            st.metric("Total Job Cards", summary_data['Total Job Cards'])
        if 'Days Covered' in summary_data:
            st.metric("Days Covered", summary_data['Days Covered'])

    with col2:
        if 'Unique Spare Parts' in summary_data:
            st.metric("Unique Spare Parts", summary_data['Unique Spare Parts'])
        if 'Total Spare Parts Used' in summary_data:
            st.metric("Total Parts Used", summary_data['Total Spare Parts Used'])

    with col3:
        if 'Unique Fault Types' in summary_data:
            st.metric("Unique Fault Types", summary_data['Unique Fault Types'])
        if 'Total Fault Occurrences' in summary_data:
            st.metric("Total Fault Occurrences", summary_data['Total Fault Occurrences'])

    with col4:
        if 'Total Quantity Required' in summary_data:
            st.metric("Total Quantity", summary_data['Total Quantity Required'])
        if 'Avg Quantity per Job' in summary_data:
            st.metric("Avg Qty/Job", summary_data['Avg Quantity per Job'])

    # Display additional insights
    insights = []
    if 'Date Range' in summary_data:
        insights.append(f"📅 **Period:** {summary_data['Date Range']}")
    if 'Most Used Spare Part' in summary_data:
        insights.append(f"🔧 **Top Spare Part:** {summary_data['Most Used Spare Part']}")
    if 'Most Common Fault' in summary_data:
        insights.append(f"⚠️ **Most Common Fault:** {summary_data['Most Common Fault']}")

    if insights:
        st.markdown("**Key Insights:**")
        for insight in insights:
            st.markdown(f"- {insight}")


def generate_combined_summary(all_vehicles_data):
    """Generate a combined summary across all selected vehicles."""
    if not all_vehicles_data:
        return

    st.markdown("## 📈 Combined Fleet Summary")

    # Aggregate data
    total_job_cards = sum(data.get('Total Job Cards', 0) for data in all_vehicles_data)
    total_spare_parts = sum(data.get('Total Spare Parts Used', 0) for data in all_vehicles_data)
    total_unique_spares = sum(data.get('Unique Spare Parts', 0) for data in all_vehicles_data)
    total_faults = sum(data.get('Total Fault Occurrences', 0) for data in all_vehicles_data)
    total_unique_faults = sum(data.get('Unique Fault Types', 0) for data in all_vehicles_data)

    # Display fleet metrics
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Vehicles", len(all_vehicles_data))
    with col2:
        st.metric("Total Job Cards", total_job_cards)
    with col3:
        st.metric("Total Parts Used", total_spare_parts)
    with col4:
        st.metric("Total Faults", total_faults)
    with col5:
        avg_jobs_per_vehicle = round(total_job_cards / len(all_vehicles_data), 1) if all_vehicles_data else 0
        st.metric("Avg Jobs/Vehicle", avg_jobs_per_vehicle)

    # Fleet insights
    st.markdown("**Fleet Insights:**")

    if total_spare_parts > 0:
        avg_parts_per_job = round(total_spare_parts / total_job_cards, 2) if total_job_cards > 0 else 0
        st.markdown(f"- 🔧 Average **{avg_parts_per_job}** spare parts per job card")

    if total_faults > 0:
        avg_faults_per_job = round(total_faults / total_job_cards, 2) if total_job_cards > 0 else 0
        st.markdown(f"- ⚠️ Average **{avg_faults_per_job}** faults per job card")

    if total_unique_spares > 0:
        st.markdown(f"- 📦 **{total_unique_spares}** different spare parts across all vehicles")

    if total_unique_faults > 0:
        st.markdown(f"- 🔍 **{total_unique_faults}** different fault types across all vehicles")


@st.dialog("📦 Spare Parts & Fault History", width="large")
def generate_spare_parts_prediction(selected_vehicles):
    st.subheader("📦 Spare Parts & Fault History (Job Card)")

    # Store summary data for combined analysis
    all_vehicles_summary = []

    for vehicle_key in selected_vehicles:
        eqpt_id, regn_no = get_vehicle_data_by_key(vehicle_key)
        regn, nomen, history_df = get_recent_jobcard_data(regn_no)

        st.markdown(f"### 🚗 {regn} - {nomen}")

        if history_df is not None and not history_df.empty:
            # Generate and display summary
            summary_data = generate_vehicle_summary(history_df, regn)
            if summary_data:
                display_summary_metrics(summary_data, regn)
                all_vehicles_summary.append(summary_data)

            st.markdown("---")

            # Show detailed tabular data
            st.markdown("#### 📋 Detailed Job Card History")
            columns_to_show = ["JobCardNo", "JobCardDate", "Faults", "itemname", "QuantityRequired"]
            display_df = history_df[columns_to_show] if all(
                col in history_df.columns for col in columns_to_show) else history_df

            st.dataframe(display_df, use_container_width=True)
            download_excel_button(display_df, filename=f"{regn}_spares_faults.xlsx")

            # 🥧 Pie Chart: Spare Parts
            if 'itemname' in history_df.columns:
                spares_series = history_df['itemname'].dropna().str.split(';').explode().str.strip()
                spare_counts = spares_series.value_counts().reset_index()
                spare_counts.columns = ['Spare Part', 'Count']

                if not spare_counts.empty:
                    st.markdown("#### 🥧 Top Spare Parts Used")
                    fig = px.pie(
                        spare_counts.head(8),
                        names='Spare Part',
                        values='Count',
                        title='Most Used Spare Parts',
                        hole=0.4
                    )
                    st.plotly_chart(fig, use_container_width=True)

            # 🥧 Pie Chart: Faults
            if 'Faults' in history_df.columns:
                fault_series = history_df['Faults'].dropna().str.split(';').explode().str.strip()
                fault_counts = fault_series.value_counts().reset_index()
                fault_counts.columns = ['Fault', 'Count']

                if not fault_counts.empty:
                    st.markdown("#### 🥧 Frequent Faults")
                    fig2 = px.pie(
                        fault_counts.head(8),
                        names='Fault',
                        values='Count',
                        title='Most Common Faults',
                        hole=0.4
                    )
                    st.plotly_chart(fig2, use_container_width=True)

        else:
            st.warning("No job card history available.")
        st.markdown("---")

    # Display combined summary if multiple vehicles
    if len(selected_vehicles) > 1:
        generate_combined_summary(all_vehicles_summary)