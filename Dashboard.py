
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
BACKGROUND_COLOR = 'white'
COLOR = 'black'

def set_page_container_style(
        max_width: int = 1100, max_width_100_percent: bool = False,
        padding_top: int = 1, padding_right: int = 10, padding_left: int = 1, padding_bottom: int = 10,
        color: str = COLOR, background_color: str = BACKGROUND_COLOR,
    ):
        if max_width_100_percent:
            max_width_str = f'max-width: 100%;'
        else:
            max_width_str = f'max-width: {max_width}px;'
        st.markdown(
            f'''
            <style>
                .reportview-container .sidebar-content {{
                    padding-top: {padding_top}rem;
                }}
                .reportview-container .main .block-container {{
                    {max_width_str}
                    padding-top: {padding_top}rem;
                    padding-right: {padding_right}rem;
                    padding-left: {padding_left}rem;
                    padding-bottom: {padding_bottom}rem;
                }}
                .reportview-container .main {{
                    color: {color};
                    background-color: {background_color};
                }}
            </style>
            ''',
            unsafe_allow_html=True,
        )

st.markdown('<style>' + open('./style.css').read() + '</style>', unsafe_allow_html=True)
with st.sidebar:
    tabs = on_hover_tabs(tabName=['Dashboard', 'Analysis', 'Economy'], 
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
set_page_container_style()
@st.cache  
def get_model_count():
     with my_cnx.cursor() as my_cur:
          my_cur.execute("select model_type,count(distinct model_name) from DEV_RAW.PUBLIC.DBT_MAPPING group by model_type")
          return my_cur.fetchall()
@st.cache
def get_model_type():
     with my_cnx.cursor() as my_cur:
          my_cur.execute("select distinct model_type from DEV_RAW.PUBLIC.DBT_MAPPING order by model_type")
          return my_cur.fetchall()
@st.cache
def get_model_business_by_type(model_type):
     with my_cnx.cursor() as my_cur:
          my_cur.execute("select distinct model_business from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type+"' order by model_business")
          return my_cur.fetchall()
@st.cache
def get_model_name_by_business(model_type,model_business):
     with my_cnx.cursor() as my_cur:
          my_cur.execute("select distinct model_name from DEV_RAW.PUBLIC.DBT_MAPPING where model_type='"+model_type+"' and model_business='"+model_business+"' order by model_name")
          return my_cur.fetchall()
@st.cache
def get_model_name_by_ref(ref):
     with my_cnx.cursor() as my_cur:
          my_cur.execute("select distinct model_name  from DEV_RAW.PUBLIC.DBT_MAPPING where model_ref_by in ("+ref+")")
          return my_cur.fetchall()

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
               return 'END' 
my_cnx = snowflake.connector.connect(**st.secrets["snowflake"])
if tabs == 'Dashboard':
     
     st.title("DBT-Snowflake Dashboard")
     df=pd.DataFrame(get_model_count())
     df.columns=['model_name','model_count']
     df = df.reset_index() 
     for index, row in df.iterrows():
          if row['model_name']=='clean':
               clean_count=row['model_count']
          if row['model_name']=='base':
               base_count=row['model_count']
          if row['model_name']=='mdl':
               mdl_count=row['model_count']
          if row['model_name']=='outbound_sds':
               outbound_count=row['model_count']
          if row['model_name']=='aggregate':
               agg_count=row['model_count']
          if row['model_name']=='enterprise':
               ent_count=row['model_count']
     col1, col2, col3, col4, col5, col6 = st.columns(6)
     col1.metric("Clean", clean_count, "1.2 °F")
     col2.metric("Base", base_count, "-8%")
     col6.metric("Enterprise", ent_count, "4%")
     col4.metric("MDL", mdl_count, "-8%")
     col5.metric("Aggregate", agg_count, "4%")
     col3.metric("Outbound", outbound_count, "4%")
                                          
elif tabs=='Analysis':
     
     col1, col2,col3 = st.columns(3)
     with col1:
          model_type=pd.DataFrame(get_model_type())
          model_type_opt=st.selectbox("Model Type",(model_type))
     with col2:
          model_business=pd.DataFrame(get_model_business_by_type(model_type_opt))
          model_business_opt=st.selectbox("Model Business",(model_business))
     with col3:
          model_list=pd.DataFrame(get_model_name_by_business(model_type_opt,model_business_opt))
          model_list_opt=st.multiselect("Model List",(model_list))
 
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
     #st.text(material)
     if len(model_list_opt):
          ref=', '.join(f'\'{w}\'' for w in model_list_opt)         
          df=pd.DataFrame(get_model_name_by_ref(ref))
          df.columns = ["model_name"]
          list_ref=df['model_name'].tolist()
          res=[]
          graph = graphviz.Digraph()

          with st.spinner('Processing ...'):
               dag(model_list_opt)
               st.graphviz_chart(graph)  
               #st.text(res)
          result=', '.join(f'\'{w}\'' for w in res) 

          with my_cnx.cursor() as my_cur:
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
               full_list=['']
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
                         full_model = st.checkbox('full_refresh',key=model+'_f',value=False)
                         if full_model:
                              full_list.append(model)

               #st.text(run_list)
               #st.text(full_list)
               dbt_run= 'dbt run --models '
               dbt_full_run= 'dbt run --models'
               for i in run_list:
                    for j in full_list:
                         if j==i:
                              dbt_full_run = dbt_full_run +' '+i
                         else:
                              dbt_run=dbt_run+' '+i
               dbt_full_run = dbt_full_run +' --full-refresh'
               #st.text('DBT RUN :'+dbt_run)
               st.code(dbt_run, language='python')
               st.code(dbt_full_run, language='python')
               #st.text('DBT FULL RUN :'+dbt_full_run)
               
                    
               
               
     
