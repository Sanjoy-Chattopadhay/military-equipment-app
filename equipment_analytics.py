import streamlit as st
import plotly.express as px
import pandas as pd

def create_pie_chart(data, column, title):
    """Create pie chart for data visualization"""
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

def generate_equipment_analytics(eqpt_df, selected_year=None):
    """Generate analytics charts for equipment data"""
    if eqpt_df.empty:
        st.warning("No data available for analytics")
        return

    # Create analytics charts
    col1, col2 = st.columns(2)

    with col1:
        if 'respecttovintage' in eqpt_df.columns:
            fig1 = create_pie_chart(eqpt_df, 'respecttovintage', 'Vintage Reliability Distribution')
            st.plotly_chart(fig1, use_container_width=True)

    with col2:
        if 'respecttodistance' in eqpt_df.columns:
            fig2 = create_pie_chart(eqpt_df, 'respecttodistance', 'Distance Reliability Distribution')
            st.plotly_chart(fig2, use_container_width=True)

    col3, col4 = st.columns(2)

    with col3:
        if 'respecttocriticalfaults' in eqpt_df.columns:
            fig3 = create_pie_chart(eqpt_df, 'respecttocriticalfaults', 'Critical Fault Reliability')
            st.plotly_chart(fig3, use_container_width=True)

    with col4:
        if 'priority' in eqpt_df.columns:
            fig4 = create_pie_chart(eqpt_df, 'priority', 'Priority Distribution')
            st.plotly_chart(fig4, use_container_width=True)

    # Summary statistics
    st.subheader("Summary Statistics")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Equipment", len(eqpt_df))

    with col2:
        if 'totalfaultcount' in eqpt_df.columns:
            avg_faults = eqpt_df['totalfaultcount'].mean()
            st.metric("Avg Faults", f"{avg_faults:.1f}")

    with col3:
        if 'totalcriticalfaultcount' in eqpt_df.columns:
            total_critical = eqpt_df['totalcriticalfaultcount'].sum()
            st.metric("Total Critical Faults", int(total_critical))

    with col4:
        if 'inkm' in eqpt_df.columns:
            avg_km = eqpt_df['inkm'].mean()
            st.metric("Avg Distance (km)", f"{avg_km:,.0f}")

    # Additional year-based filtering info
    if selected_year and selected_year != "All":
        st.info(f"Analytics filtered for equipment issued from year {selected_year} onwards")
    else:
        st.info("Analytics showing all equipment (no year filter applied)")
