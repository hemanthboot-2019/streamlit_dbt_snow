
import streamlit as st
import pandas as pd
import numpy as np
import requests
import snowflake.connector
import graphviz as graphviz
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
@st.cache(ttl=3600)
def get_clean_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='clean'")
    return my_cur.fetchall()
@st.cache(ttl=3600)
def get_base_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='base'")
    return my_cur.fetchall()
@st.cache(ttl=3600)
def get_enterprise_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='enterprise'")
    return my_cur.fetchall()
@st.cache(ttl=3600)
def get_mdl_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='mdl'")
    return my_cur.fetchall()
@st.cache(ttl=3600)
def get_outbound_count():
  with my_cnx.cursor() as my_cur:
    my_cur.execute("select distinct(model_name) from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='outbound_sds'")
    return my_cur.fetchall()
@st.cache(ttl=3600)
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

def dag(input_array):
     objects=', '.join(f'\'{w}\'' for w in input_array)
     with my_cnx.cursor() as my_cur:
          my_cur.execute(" select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+objects+")")
          #df =pd.DataFrame(pd.np.empty((0, 1)))
          df=pd.DataFrame(my_cur.fetchall())
          if len(df.columns)>0:
               df.columns = ["model_name"]
               #st.text(df)
               list_ref=df['model_name'].tolist()
               #st.text(list_ref)
               my_cur.execute(" select distinct model_name,model_ref_by  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+objects+")")
               #fd=my_cur.fetchall()
               df = pd.DataFrame(my_cur.fetchall())
               df.columns = ["model_name","model_ref_by"]
               df = df.reset_index()  # make sure indexes pair with number of rows

               for index, row in df.iterrows():
                    st.text(row['model_name']+ row['model_ref_by'])
               
               res.append(fd)
               
               
               if len(df)>0:
                    dag(list_ref)
          return "end"
     
             
          
          

my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cur.execute("select distinct model_type  from DEV_RAW.PUBLIC.DBT_MAPPING")
     model_type=pd.DataFrame(my_cur.fetchall())
     col1, col2,col3 = st.columns(3)
     with col1:
          model_type_opt=st.selectbox("Model Type",(model_type))
          st.text(model_type_opt)
     with col2:
          my_cur.execute("select distinct model_business from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"'")
          model_business=pd.DataFrame(my_cur.fetchall())
          model_business_opt=st.selectbox("Model Business",(model_business))
          st.text(model_business_opt)
     with col3:
          my_cur.execute("select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"' and model_business='"+model_business_opt+"'")
          model_list=pd.DataFrame(my_cur.fetchall())
          model_list_opt=st.multiselect("Model List",(model_list))
          st.text(model_list_opt)
          
     
     if st.button('Analyse Impact'):
          st.text("button clicekd")
          result=', '.join(f'\'{w}\'' for w in model_list_opt)
          st.text(result)
          #my_cur.execute("select model_ref_by,model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+result+")")
          my_cur.execute(" select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+result+")")
          df=pd.DataFrame(my_cur.fetchall())
          df.columns = ["model_name"]
          st.text(df)
          list_ref=df['model_name'].tolist()
          st.text(list_ref)
          
          res=list()
          dag(model_list_opt)
          st.text(res)
          #st.text( "select model_name, model_ref_by from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"' and model_business='"+model_business_opt+"'and model_name in ("+result+")")
     else :
          st.text("button not clicked")
