
import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import snowflake.connector
import graphviz as graphviz
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


my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])

st.title("DBT-Snowflake Dashboard")


def dag(input_array):
     objects=', '.join(f'\'{w}\'' for w in input_array)
     with my_cnx.cursor() as my_cur:
          my_cur.execute(" select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+objects+")")
          df=pd.DataFrame(my_cur.fetchall())
          if len(df.columns)>0:
               df.columns = ["model_name"]
               list_ref=df['model_name'].tolist()
               my_cur.execute(" select distinct model_name,model_ref_by  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+objects+")")
               df = pd.DataFrame(my_cur.fetchall())
               df.columns = ["model_name","model_ref_by"]
               df = df.reset_index() 
               for index, row in df.iterrows():
                    graph.edge(row['model_ref_by'], row['model_name'])
                    res.append(row['model_name'])
               if len(df)>0:
                    dag(list_ref)
          return "end"
 
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
with my_cnx.cursor() as my_cur:
     my_cur.execute("select distinct model_type  from DEV_RAW.PUBLIC.DBT_MAPPING order by model_type ")
     model_type=pd.DataFrame(my_cur.fetchall())
     col1, col2,col3 = st.columns(3)
     with col1:
          model_type_opt=st.selectbox("Model Type",(model_type))
          st.text(model_type_opt)
     with col2:
          my_cur.execute("select distinct model_business from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"' order by model_business ")
          model_business=pd.DataFrame(my_cur.fetchall())
          model_business_opt=st.selectbox("Model Business",(model_business))
          st.text(model_business_opt)
     with col3:
          my_cur.execute("select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"' and model_business='"+model_business_opt+"' order by model_name")
          model_list=pd.DataFrame(my_cur.fetchall())
          model_list_opt=st.multiselect("Model List",(model_list))
          st.text(model_list_opt)
          
     col0,col1,col2,col3 =st.columns(4)
     with col0:
          st.text('Materialization :')
     with col1:
          view = st.checkbox('View',value=True)
     with col2:
          table = st.checkbox('Table',value=True)
     with col3:
          incremental = st.checkbox('Incremental',value=True)
     materialize=[]
     if view:
          
          materialize.append('view')
     if table:
          materialize.append('table')
     if incremental:
          materialize.append('incremental')
     material=', '.join(f'\'{w}\'' for w in materialize)      
     st.text(material)
     if len(model_list_opt):
          result=', '.join(f'\'{w}\'' for w in model_list_opt)         
          my_cur.execute(" select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+result+")")
          df=pd.DataFrame(my_cur.fetchall())
          df.columns = ["model_name"]
          list_ref=df['model_name'].tolist()
          res=[]
          graph = graphviz.Digraph()

          with st.spinner('Processing ...'):
               dag(model_list_opt)
               st.graphviz_chart(graph)
               #st.text(res)
          result=', '.join(f'\'{w}\'' for w in res) 


          col1,col2,col3 =st.columns(3)
          with col1:
               st.text('CLEAN')
               my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and model_type ='clean' and CONFIG_MATERIALIZATION in ("+material+")")
               st.dataframe(my_cur.fetchall())
          with col2:
               st.text('BASE')
               my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and model_type ='base' and CONFIG_MATERIALIZATION in ("+material+")")
               st.dataframe(my_cur.fetchall())
          with col3:
               st.text('MDL')
               my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and model_type ='mdl' and CONFIG_MATERIALIZATION in ("+material+")")
               st.dataframe(my_cur.fetchall())
          col4,col5,col6 =st.columns(3)
          with col4:
               st.text('AGGREGATE')
               my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and model_type ='aggregate' and CONFIG_MATERIALIZATION in ("+material+")")
               st.dataframe(my_cur.fetchall())
          with col5:
               st.text('OUTBOUND')
               my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and model_type ='outbound_sds' and CONFIG_MATERIALIZATION in ("+material+")")
               st.dataframe(my_cur.fetchall())
          with col6:
               st.text('ENTERPRISE')
               my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and model_type ='enterprise' and CONFIG_MATERIALIZATION in ("+material+")")
               st.dataframe(my_cur.fetchall())
          my_cur.execute(" select distinct nvl(model_name,'NA')  from DEV_RAW.PUBLIC.DBT_MAPPING where model_name in ("+result+") and CONFIG_MATERIALIZATION in ("+material+")")
          df = pd.DataFrame(my_cur.fetchall())
          df.columns = ["model_name"]
          df = df.reset_index() 

          for index, row in df.iterrows():
               model=row['model_name']
               col1,col2,col3 =st.columns(3)
               with col1:
                    st.text(model)
               with col2:
                    st.checkbox('run',key=model)
               with col3:
                    st.checkbox('full_refresh',key=model+'_f')
                    
               
               
     
