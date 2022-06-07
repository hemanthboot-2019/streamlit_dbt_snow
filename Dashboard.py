
import streamlit as st
import pandas as pd
import requests
import snowflake.connector
from urllib.error import URLError
# 📜
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

def get_clean_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='clean'")
    return my_cur.fetchall()
def get_base_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='base'")
    return my_cur.fetchall()
def get_enterprise_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='enterprise'")
    return my_cur.fetchall()
def get_mdl_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='mdl'")
    return my_cur.fetchall()
def get_outbound_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='outbound_sds'")
    return my_cur.fetchall()
def get_aggregate_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='aggregate'")
    return my_cur.fetchall()
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
clean_count=get_clean_count()
clean_count=pd.DataFrame(clean_count)
base_count=get_base_count()
base_count=pd.DataFrame(base_count)
enterprise_count=get_enterprise_count()
enterprise_count=pd.DataFrame(enterprise_count)
mdl_count=get_mdl_count()
mdl_count=pd.DataFrame(mdl_count)
outbound_count=get_outbound_count()
outbound_count=pd.DataFrame(outbound_count)
aggregate_count=get_aggregate_count()
aggregate_count=pd.DataFrame(aggregate_count)
st.title("DBT-Snowflake Dashboard")
col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Clean", len(clean_count), "1.2 °F")
col2.metric("Base", len(base_count), "-8%")
col6.metric("Enterprise", len(enterprise_count), "4%")
col4.metric("MDL", len(mdl_count), "-8%")
col5.metric("Aggregate", len(aggregate_count), "4%")
col3.metric("Outbound", len(outbound_count), "4%")
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cur.execute("select distinct model_type  from DEV_RAW.PUBLIC.DBT_MAPPING")
     model_type=pd.DataFrame(my_cur.fetchall())
     col1, col2 = st.columns(2)
     with col1:
          model_type_opt=st.selectbox("Model Type",(model_type))
          st.text(model_type_opt)
     with col2:
          my_cur.execute("select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"'")
          model_list=pd.DataFrame(my_cur.fetchall())
          model_list_opt=st.selectbox("Model List",(model_list))
          st.text(model_type_opt)
          
     
