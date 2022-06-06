
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
#@streamlit.cache  
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
def get_clean_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select * from DEV_RAW.PUBLIC.DBT_MAPPING_DF where model_type='clean'")
    return my_cur.fetchall()
clean_count=get_clean_count()
clean_count=pd.DataFrame(clean_count)
st.title("DBT-Snowflake Dashboard")
col1, col2, col3 = st.columns(3)
col1.metric("Clean", len(clean_count), "1.2 °F")
col2.metric("Base", "9 mph", "-8%")
col3.metric("Enterprise", "86%", "4%")
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cur.execute("select * from DEV_RAW.PUBLIC.DBT_MAPPING_DF")
     st.dataframe(my_cur.fetchall())
