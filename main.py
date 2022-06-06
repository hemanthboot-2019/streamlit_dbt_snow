
import streamlit as st
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

st.set_page_config(
     page_title="AI-Support",
     page_icon="❄️",
     layout="wide",
     initial_sidebar_state="expanded",
     menu_items={
         'Get Help': 'https://www.extremelycoolapp.com/help',
         'Report a bug': "https://www.extremelycoolapp.com/bug",
         'About': "# This is a header. This is an *extremely* cool app!"
     }
 )
st.title("DBT-Snowflake Dashboard")
col1, col2, col3 = st.columns(3)
col1.metric("Clean", "70 °F", "1.2 °F")
col2.metric("Base", "9 mph", "-8%")
col3.metric("Enterprise", "86%", "4%")
my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cur.execute("select * from DEV_RAW.PUBLIC.DBT_MAPPING_DF")
     st.dataframe(my_cur.fetchall())
