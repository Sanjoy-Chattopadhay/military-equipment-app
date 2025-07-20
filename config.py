import os
from sqlalchemy import create_engine
import streamlit as st

# PostgreSQL/Neon DB connection
NEON_CONNECTION_STRING = "postgresql://neondb_owner:npg_ZgkW9VU8dBcY@ep-holy-cell-a7vl9851-pooler.ap-southeast-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

@st.cache_resource
def get_connection():
    """Create and return PostgreSQL connection"""
    return create_engine(NEON_CONNECTION_STRING)

# Theme configuration
def apply_theme(theme_type):
    """Apply CSS theme"""
    if theme_type == "dark":
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
