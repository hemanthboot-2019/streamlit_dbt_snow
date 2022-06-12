
import streamlit as st
import pandas as pd
import numpy as np
import requests
import time
import snowflake.connector
import graphviz as graphviz
from urllib.error import URLError
from st_on_hover_tabs import on_hover_tabs

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
st.markdown('<style>' + open('./style.css').read() + '</style>', unsafe_allow_html=True)
with st.sidebar:
    tabs = on_hover_tabs(tabName=['Dashboard', 'Impact', 'Economy'], 
                         iconName=['dashboard', 'insights', 'economy'],
                         styles = {'navtab': {'background-color':'#111',
                                                  'color': '#818181',
                                                  'font-size': '18px',
                                                  'transition': '.3s',
                                                  'white-space': 'nowrap',
                                                  'text-transform': 'uppercase'},
                                       'tabOptionsStyle': {':hover :hover': {'color': 'red',
                                                                      'cursor': 'pointer'}},
                                       'iconStyle':{'position':'fixed',
                                                    'left':'7.5px',
                                                    'text-align': 'left'},
                                       'tabStyle' : {'list-style-type': 'none',
                                                     'margin-bottom': '30px',
                                                     'padding-left': '30px'}},
                             default_choice=0)
#@streamlit.cache  

if tabs == 'Dashboard':
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
     with my_cnx.cursor() as my_cur:
          my_cur.execute("select * from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='mdl'")
          df=pd.DataFrame(my_cur.fetchall())
          df.to_csv('./export.csv')

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
elif tabs=='Impact':
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
          my_cur.execute("select distinct model_type  from DEV_RAW.PUBLIC.DBT_MAPPING order by model_type")
          model_type=pd.DataFrame(my_cur.fetchall())
          col1, col2,col3 = st.columns(3)
          with col1:
               model_type_opt=st.selectbox("Model Type",(model_type))
               st.text(model_type_opt)
          with col2:
               my_cur.execute("select distinct model_business from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type_opt+"' order by model_business")
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
               run_list=[]
               full_list=[]
               for index, row in df.iterrows():
                    model=row['model_name']
                    col1,col2,col3 =st.columns(3)
                    with col1:
                         st.text(model)
                    with col2:
                         run_model = st.checkbox('run',key=model)
                         if run_model:
                              run_list.append(model)
                    with col3:
                         full_model = st.checkbox('full_refresh',key=model+'_f')
                         if full_model:
                              full_list.append(model)
                         
               st.text(run_list)
               st.text(full_list)
               dbt_run= 'dbt run --models '
               dbt_full_run= 'dbt run --models'
               for i in run_list:
                    for j in full_list:
                         if j==i:
                              dbt_full_run = dbt_full_run +' '+i
                         else:
                              dbt_run=dbt_run+' '+i
               dbt_full_run = dbt_full_run +' --full-refresh'
               st.text('DBT RUN :'+dbt_run)
               st.text('DBT FULL RUN :'+dbt_full_run)
               
                    
               
               
     
