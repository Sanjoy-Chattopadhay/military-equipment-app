import streamlit as st
import pandas as pd


def initialize_session_state():
    """Initialize session state variables"""
    if "theme" not in st.session_state:
        st.session_state.theme = "light"

    if 'show_modal' not in st.session_state:
        st.session_state.show_modal = False

    if 'selected_equipment' not in st.session_state:
        st.session_state.selected_equipment = None

    if 'selected_vehicles' not in st.session_state:
        st.session_state.selected_vehicles = []

    if 'show_analytics' not in st.session_state:
        st.session_state.show_analytics = False


def load_excel_km_data(eqpt_df):
    """Load and merge Excel InKm data"""
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

        return eqpt_df
    except FileNotFoundError:
        st.warning("RegnInKm.xlsx not found. Using database InKm values only.")
        eqpt_df['inkm'] = pd.to_numeric(eqpt_df['inkm'], errors='coerce').fillna(0).astype(int)
        return eqpt_df
    except Exception as e:
        st.error(f"Error loading Excel data: {e}")
        eqpt_df['inkm'] = pd.to_numeric(eqpt_df['inkm'], errors='coerce').fillna(0).astype(int)
        return eqpt_df
