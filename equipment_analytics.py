import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime

def generate_equipment_analytics(eqpt_df, selected_year=None):
    if eqpt_df is None or eqpt_df.empty:
        st.warning("No data available for analytics.")
        return

    # --- Preprocessing ---
    eqpt_df['JobCardDate'] = pd.to_datetime(eqpt_df.get('JobCardDate', pd.Series()), errors='coerce')

    if selected_year and selected_year != "All":
        eqpt_df = eqpt_df[eqpt_df['JobCardDate'].dt.year >= int(selected_year)]

    # Add Age if missing
    if 'Year' in eqpt_df.columns and 'Age' not in eqpt_df.columns:
        eqpt_df['Age'] = datetime.now().year - eqpt_df['Year']

    st.title("📊 Equipment Analytics Dashboard")
    st.markdown("---")

    # --- Tabs ---
    tab1, tab2, tab3 = st.tabs(["📈 Priority & Reliability", "🔧 Fault Analysis", "🚗 Equipment Details"])

    # --- Tab 1: Priority & Reliability ---
    with tab1:
        st.subheader("Priority Distribution")
        priority_counts = eqpt_df['Priority'].value_counts()
        fig_priority = px.pie(priority_counts,
                              values=priority_counts.values,
                              names=priority_counts.index,
                              title="Equipment Priority Distribution",
                              color_discrete_map={
                                  'P1':'#2E8B57','P2':'#FFD700','P3':'#FFA500',
                                  'P4':'#FF6347','P5':'#8B0000'
                              })
        st.plotly_chart(fig_priority, use_container_width=True, key="priority_pie")

        st.subheader("Priority vs Distance")
        fig_scatter = px.scatter(
            eqpt_df,
            x='inkm',
            y='Priority',
            size='TotalFaultCount',
            color='RespectToDistance',
            hover_data=['regnno', 'nomenclature']
        )
        st.plotly_chart(fig_scatter, use_container_width=True, key="scatter_priority_distance")

    # --- Tab 2: Fault Analysis ---
    with tab2:
        st.subheader("Total Faults Distribution")
        fault_bins = pd.cut(eqpt_df['TotalFaultCount'], bins=[0,5,10,20,50,float('inf')],
                            labels=['0-5','6-10','11-20','21-50','50+'])
        fault_counts = fault_bins.value_counts()
        fig_fault = px.bar(x=fault_counts.index, y=fault_counts.values, title="Equipment by Fault Count Range")
        st.plotly_chart(fig_fault, use_container_width=True, key="fault_bar")

        st.subheader("Top 10 Equipment by Total Faults")
        top_faults = eqpt_df.nlargest(10, 'TotalFaultCount')[['regnno','TotalFaultCount']]
        fig_top = px.bar(top_faults,
                         x='TotalFaultCount',
                         y='regnno',
                         orientation='h',
                         title="Top 10 Equipment by Faults")
        st.plotly_chart(fig_top, use_container_width=True, key="top10_faults")

    # --- Tab 3: Equipment Details ---
    with tab3:
        st.subheader("Distance vs Age")
        fig_age = px.scatter(eqpt_df, x='Age', y='inkm', color='Priority', size='TotalFaultCount',
                             hover_data=['regnno', 'nomenclature'],
                             title="Equipment Distance vs Age")
        st.plotly_chart(fig_age, use_container_width=True, key="scatter_age_distance")

        st.subheader("Equipment Type Distribution")
        if 'Nomenclature' in eqpt_df.columns:
            top_types = eqpt_df['Nomenclature'].value_counts().head(10)
            fig_types = px.bar(x=top_types.index, y=top_types.values, title="Top 10 Equipment Types")
            st.plotly_chart(fig_types, use_container_width=True, key="top10_equipment_types")

    # --- Summary Metrics ---
    st.markdown("---")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Equipment", len(eqpt_df))
    col2.metric("Average Faults", f"{eqpt_df['TotalFaultCount'].mean():.1f}")
    col3.metric("Critical Equipment (P4/P5)", len(eqpt_df[eqpt_df['Priority'].isin(['P4','P5'])]))

    st.success("🎉 Analytics dashboard generated successfully!")
