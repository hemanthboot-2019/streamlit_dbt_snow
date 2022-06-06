
import streamlit
import pandas
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
my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cnr.execute("select * from DEV_RAW.PUBLIC.DBT_MAPPING_DF")
     streamlit.text(my_cnx.fetchall())
