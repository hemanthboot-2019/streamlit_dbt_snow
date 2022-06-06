
import streamlit
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError

streamlit.set_page_config(
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
streamlit.title("DBT-Snowflake Dashboard")
col1, col2, col3 = st.columns(3)
col1.metric("Temperature", "70 °F", "1.2 °F")
col2.metric("Wind", "9 mph", "-8%")
col3.metric("Humidity", "86%", "4%")
my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cur.execute("select * from DEV_RAW.PUBLIC.DBT_MAPPING_DF")
     streamlit.dataframe(my_cur.fetchall())
