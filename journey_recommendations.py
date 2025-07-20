import streamlit as st
import pandas as pd
from sqlalchemy import text

# Use the same connection as main.py
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ZgkW9VU8dBcY@ep-holy-cell-a7vl9851-pooler.ap-southeast-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


@st.cache_resource
def get_journey_connection():
    from sqlalchemy import create_engine
    return create_engine(NEON_CONNECTION_STRING)


def get_vehicle_fault_patterns(vehicle_keys):
    """Get fault patterns for selected vehicles"""
    engine = get_journey_connection()

    # Extract EqptIDs from vehicle keys
    eqpt_ids = []
    for key in vehicle_keys:
        eqpt_id = key.split('_')[0]
        eqpt_ids.append(int(eqpt_id))

    if not eqpt_ids:
        return pd.DataFrame()

    # Create placeholders for the IN clause
    placeholders = ','.join([f':id{i}' for i in range(len(eqpt_ids))])

    query = f"""
        SELECT 
            r.id as eqptid,
            r.regnno,
            r.nomenclature,
            f.faults,
            COUNT(*) as fault_frequency
        FROM teqptrecord r
        LEFT JOIN jobcard jc ON r.id = jc.referid
        LEFT JOIN jobcarddetails jcd ON jc.id = jcd.refjobno
        LEFT JOIN tfaults f ON jcd.fault = f.faultid
        WHERE r.id IN ({placeholders})
        AND f.faults IS NOT NULL
        GROUP BY r.id, r.regnno, r.nomenclature, f.faults
        ORDER BY r.regnno, fault_frequency DESC
    """

    # Create parameter dictionary
    params = {f'id{i}': eqpt_id for i, eqpt_id in enumerate(eqpt_ids)}

    return pd.read_sql(query, engine, params=params)


def get_spare_parts_for_faults(fault_patterns):
    """Get spare parts used for common faults"""
    engine = get_journey_connection()

    if fault_patterns.empty:
        return pd.DataFrame()

    # Get unique faults
    faults = fault_patterns['faults'].unique().tolist()

    if not faults:
        return pd.DataFrame()

    # Create placeholders for fault names
    placeholders = ','.join([f':fault{i}' for i in range(len(faults))])

    query = f"""
        SELECT 
            f.faults,
            sm.itemname as spare_part,
            COUNT(*) as usage_frequency
        FROM tfaults f
        LEFT JOIN jobcarddetails jcd ON f.faultid = jcd.fault
        LEFT JOIN jobcard jc ON jcd.refjobno = jc.id
        LEFT JOIN tsstransactionregister tr ON jc.id = tr.refjobid
        LEFT JOIN tssstockmaster sm ON tr.partnoid = sm.id
        WHERE f.faults IN ({placeholders})
        AND sm.itemname IS NOT NULL
        AND tr.issues > 0
        GROUP BY f.faults, sm.itemname
        ORDER BY f.faults, usage_frequency DESC
    """

    # Create parameter dictionary
    params = {f'fault{i}': fault for i, fault in enumerate(faults)}

    return pd.read_sql(query, engine, params=params)


def generate_spare_parts_prediction(selected_vehicles):
    """Generate spare parts prediction for journey"""
    st.subheader("🔧 Journey Spare Parts Prediction")

    if not selected_vehicles:
        st.warning("No vehicles selected for spare parts prediction")
        return

    st.info(f"Analyzing {len(selected_vehicles)} selected vehicles for spare parts recommendations...")

    with st.spinner("Loading fault patterns and spare parts data..."):
        # Get fault patterns for selected vehicles
        fault_patterns = get_vehicle_fault_patterns(selected_vehicles)

        if fault_patterns.empty:
            st.warning("No fault data found for selected vehicles")
            return

        # Get spare parts for these faults
        spare_parts_data = get_spare_parts_for_faults(fault_patterns)

        if spare_parts_data.empty:
            st.warning("No spare parts data found for common faults")
            return

    # Display results in tabs
    tab1, tab2, tab3 = st.tabs(["Vehicle Fault Summary", "Recommended Spare Parts", "Critical Items"])

    with tab1:
        st.markdown("### 🚗 Selected Vehicles Fault Analysis")

        # Group fault patterns by vehicle
        vehicle_summary = fault_patterns.groupby(['eqptid', 'regnno', 'nomenclature']).agg({
            'fault_frequency': 'sum',
            'faults': 'count'
        }).reset_index()
        vehicle_summary.rename(columns={'fault_frequency': 'total_fault_occurrences', 'faults': 'unique_faults'},
                               inplace=True)

        st.dataframe(
            vehicle_summary,
            use_container_width=True,
            hide_index=True,
            column_config={
                "eqptid": st.column_config.NumberColumn("Equipment ID", format="%d"),
                "regnno": st.column_config.TextColumn("Registration No"),
                "nomenclature": st.column_config.TextColumn("Equipment Type"),
                "total_fault_occurrences": st.column_config.NumberColumn("Total Fault Occurrences", format="%d"),
                "unique_faults": st.column_config.NumberColumn("Unique Faults", format="%d")
            }
        )

        # Show top faults across all selected vehicles
        st.markdown("### 📊 Most Common Faults Across Selected Vehicles")
        top_faults = fault_patterns.groupby('faults')['fault_frequency'].sum().reset_index()
        top_faults = top_faults.sort_values('fault_frequency', ascending=False).head(10)

        st.dataframe(
            top_faults,
            use_container_width=True,
            hide_index=True,
            column_config={
                "faults": st.column_config.TextColumn("Fault Description", width="large"),
                "fault_frequency": st.column_config.NumberColumn("Total Frequency", format="%d")
            }
        )

    with tab2:
        st.markdown("### 🔧 Recommended Spare Parts for Journey")

        # Aggregate spare parts recommendations
        spare_parts_summary = spare_parts_data.groupby('spare_part')['usage_frequency'].sum().reset_index()
        spare_parts_summary = spare_parts_summary.sort_values('usage_frequency', ascending=False)

        # Add recommendation priority
        total_parts = len(spare_parts_summary)
        if total_parts > 0:
            spare_parts_summary['priority'] = spare_parts_summary['usage_frequency'].rank(method='dense',
                                                                                          ascending=False).astype(int)
            spare_parts_summary['recommendation'] = spare_parts_summary['priority'].apply(
                lambda x: 'Critical' if x <= max(1, total_parts * 0.2)
                else 'Important' if x <= max(2, total_parts * 0.5)
                else 'Optional'
            )

        st.dataframe(
            spare_parts_summary,
            use_container_width=True,
            hide_index=True,
            column_config={
                "spare_part": st.column_config.TextColumn("Spare Part", width="large"),
                "usage_frequency": st.column_config.NumberColumn("Usage Frequency", format="%d"),
                "priority": st.column_config.NumberColumn("Priority Rank", format="%d"),
                "recommendation": st.column_config.TextColumn("Recommendation Level")
            }
        )

    with tab3:
        st.markdown("### ⚠️ Critical Items Analysis")

        if not spare_parts_summary.empty:
            critical_parts = spare_parts_summary[spare_parts_summary['recommendation'] == 'Critical']

            if not critical_parts.empty:
                st.markdown("**Critical spare parts to carry:**")
                for _, part in critical_parts.iterrows():
                    st.markdown(f"• **{part['spare_part']}** (Used {part['usage_frequency']} times)")

                st.markdown("---")
                st.markdown("**Journey Recommendations:**")
                st.success(f"✅ Carry {len(critical_parts)} critical spare parts")
                st.info(
                    f"💡 Consider {len(spare_parts_summary[spare_parts_summary['recommendation'] == 'Important'])} additional important parts")

                # Calculate total weight/space recommendation (placeholder)
                st.markdown("**Logistics Planning:**")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Critical Parts", len(critical_parts))
                with col2:
                    st.metric("Important Parts",
                              len(spare_parts_summary[spare_parts_summary['recommendation'] == 'Important']))
                with col3:
                    st.metric("Total Recommendations", len(spare_parts_summary))
            else:
                st.info("No critical spare parts identified for selected vehicles")
        else:
            st.warning("No spare parts data available for analysis")

    # Add download functionality
    if not spare_parts_summary.empty:
        st.markdown("---")
        st.markdown("### 📥 Export Recommendations")

        # Create comprehensive report
        report_data = []
        for _, vehicle in vehicle_summary.iterrows():
            vehicle_faults = fault_patterns[fault_patterns['eqptid'] == vehicle['eqptid']]
            for _, fault in vehicle_faults.iterrows():
                related_parts = spare_parts_data[spare_parts_data['faults'] == fault['faults']]
                for _, part in related_parts.iterrows():
                    report_data.append({
                        'Registration_No': vehicle['regnno'],
                        'Equipment_Type': vehicle['nomenclature'],
                        'Fault': fault['faults'],
                        'Fault_Frequency': fault['fault_frequency'],
                        'Spare_Part': part['spare_part'],
                        'Part_Usage_Frequency': part['usage_frequency']
                    })

        if report_data:
            report_df = pd.DataFrame(report_data)
            csv = report_df.to_csv(index=False)

            st.download_button(
                label="Download Detailed Report (CSV)",
                data=csv,
                file_name=f"spare_parts_prediction_{len(selected_vehicles)}_vehicles.csv",
                mime="text/csv"
            )
