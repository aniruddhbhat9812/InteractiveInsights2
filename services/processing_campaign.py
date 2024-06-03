import requests
import json
# %python
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv, find_dotenv
_ = load_dotenv(find_dotenv()) # read local .env file
from langchain.chains import RetrievalQA
# from langchain.chat_models import ChatOpenAI
# from langchain.document_loaders import CSVLoader
# from langchain.vectorstores import DocArrayInMemorySearch
# from IPython.display import display, Markdown
from langchain.embeddings.openai import OpenAIEmbeddings
# from langchain.chat_models import AzureChatOpenAI
# from langchain.indexes import VectorstoreIndexCreator
# from langchain.document_loaders import JSONLoader
# from langchain.document_loaders import TextLoader
# from langchain.text_splitter import RecursiveCharacterTextSplitter, CharacterTextSplitter
# import os
# from langchain.llms import AzureOpenAI
# from langchain.llms import OpenAI
# import findspark
# findspark.init()

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)

from langchain import SQLDatabase
# from langchain_experimental.sql import SQLDatabaseChain
import psycopg2

def call_gpt2():
    from langchain.chat_models import AzureChatOpenAI
    import os

    os.environ["OPENAI_API_TYPE"]="azure"
    os.environ["OPENAI_API_VERSION"]="2023-07-01-preview"
    os.environ["OPENAI_API_BASE"]="https://tdl-aoai-southindia.openai.azure.com/" # Your Azure OpenAI resource endpoint
    os.environ["OPENAI_API_KEY"]="b34db39a21bd4d6aa73f5a12e9f6fdf9" # Your Azure OpenAI resource key
    os.environ["OPENAI_CHAT_MODEL"]="tdl-gpt-4-turbo-preview" # Use name of deployment

    embeddings1=OpenAIEmbeddings(openai_api_type="azure",openai_api_base="https://tdl-chatbot.openai.azure.com/",openai_api_version="2023-05-15", deployment="embedding-ada",model="text-embedding-ada-002", openai_api_key="b34db39a21bd4d6aa73f5a12e9f6fdf9",chunk_size=1)

    llm=AzureChatOpenAI(temperature=0.00,openai_api_base="https://tdl-aoai-southindia.openai.azure.com/",

                        openai_api_type="azure" ,model="tdl-gpt4-turbo-preview",openai_api_version="2023-07-01-preview",

                        deployment_name="tdl-gpt4-turbo-preview")
    return llm
def call_gpt():
    from langchain.chat_models import AzureChatOpenAI
    import os

    os.environ["OPENAI_API_TYPE"]="azure"
    os.environ["OPENAI_API_VERSION"]="2023-07-01-preview"
    os.environ["OPENAI_API_BASE"]="https://tdl-aoai-southindia.openai.azure.com/"# Your Azure OpenAI resource endpoint
    os.environ["OPENAI_API_KEY"]="b34db39a21bd4d6aa73f5a12e9f6fdf9" # Your Azure OpenAI resource key
    os.environ["OPENAI_CHAT_MODEL"]="tdl-gpt35-turbo" # Use name of deployment

    embeddings1=OpenAIEmbeddings(openai_api_type="azure",openai_api_base="https://tdl-chatbot.openai.azure.com/",openai_api_version="2023-05-15", deployment="embedding-ada",model="text-embedding-ada-002", openai_api_key="95a179d989cd4aeb84d36edb2fc8991a",chunk_size=1)


    llm=AzureChatOpenAI(temperature=0.0,openai_api_base="https://tdl-aoai-southindia.openai.azure.com/",

                        openai_api_type="azure" ,model="tdl-gpt35-turbo",openai_api_version="2023-07-01-preview",

                        deployment_name="tdl-gpt35-turbo")
    return llm

def postgresql_connect():
    username = "appuser" 
    password = "appuser@1234" 
    host = "psg-tdprd-pr-ci-voc-01.postgres.database.azure.com:5432/postgres?sslmode=require"
    driver = "org.postgresql.Driver"
    port = "5432"
    mydatabase = "postgres"
    pg_uri = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{mydatabase}"
    # db = SQLDatabase.from_uri(pg_uri)
    con = psycopg2.connect(
    database="tables",
    user="postgres",
    password="a162534A",
    host="localhost",
    port= '5432'
    )
    cursor_obj = con.cursor()
    return con


con = postgresql_connect()
for i in range(100):
    con.close()

tables_list = ["warehouse_prod_tcp.cx_dashboard_app_health_kpi",\
"warehouse_prod_tcp.cx_dashboard_enrollment_success_rate",\
"reviews.tataneu_ratings",\
"warehouse_prod_tcp.buisnesshealth_dashboard_avg_app_ratings",\
"warehouse_prod_tcp.app_uninstalls",\
"warehouse_prod_tcp.marketing_kpi_gold_daily",\
"daily_reporting.daily_visit_conversion_rate_fy24",\
"daily_reporting.uninstall_analysis_date_level",\
"warehouse_prod_adobe.revised_cx_brandnavigation_visit",\
"warehouse_prod_tcp.marketing_kpi_gold_daily",\
"warehouse_prod_tcp.marketing_kpi_gold_monthly",\
"warehouse_prod_adobe.search_l0_metrics_visit_level",\
"warehouse_prod_adobe.brandnavigation_combined_overall_search",\
"warehouse_prod_tcp.buiseness_health_dashboard_null_search_rate",\
"default.cx_jarvis",\
"warehouse_prod_tcp.cx_dashboard_avg_brand_transition_time_v3",\
"daily_reporting.daily_visit_conversion_rate_fy24",\
"default.cx_jarvis",\
"daily_reporting.opel_payment_success_report",\
"daily_reporting.tpm_report_with_payment_mode",\
"daily_reporting.daily_visit_conversion_rate_fy24",\
"warehouse_prod_tcp.marketing_kpi_gold_weekly",\
"warehouse_prod_tcp.marketing_kpi_gold_monthly",\
"warehouse_prod_adobe.search_lo_metrics_instance_level",\
"daily_reporting.daily_visit_conversion_rate_fy24",\
"warehouse_prod_tcp.health_for_business_kpi_3",\
"warehouse_prod_tcp.health_for_business_kpi_2",\
"warehouse_prod_tcp.cx_dashboard_customer_service_gocr",\
"warehouse_prod_tcp.customer_service_national_dashboard",\
"warehouse_prod_tcp.cx_dashboard_cs_non_ftr_resolution_time_aggregated",\
"warehouse_prod_tcp.cx_dashboard_cs_open_tickets_aggregated",\
"warehouse_prod_tcp.cs_csat_business_health_dashboard"]
sql_tables = []
create = "create"
for table in tables_list:
    try:
        table_df = str(spark.sql(f"SELECT * FROM {table}"))
        table_df = table_df.replace("DataFrame[", "").replace(":","").replace("]","")
        s = create + table + "(" + table_df + ");"
        sql_tables.append(s)
    except:
        continue
tables_metadata = ["ANR,User Perceived ANR,App Startup time (Firebase P90),Slow Cold Start,Crash Rate",\
    "Enrol Success rate,Login Success rate",\
    "App Ratings - Android /iOS",\
    "Android avg.app rating",\
    "Gross Installs : DRR,Cummulative Gross Installs,Uninstalls:DRR,Cummulative Uninstalls,%Uninstalls,Cummulative Uninstalls%,Net Installs:DRR,Cu:mmulative: Net Installs",\
    "  Organic,Pai%Organic Installs",\
    "Enrollments:DRR,Cummulative Enrollments,%Migrated,Cummulative: % Migrated,Neupass Members added,Cummulative NeuPass Members,NeuPass\(TN Enrols/NeuPass Enrols),Conversion,Total Transactions,GMV",\
    "D0 (< = 5Mins),D0,D1,D7,D30",\
    "Platform daily visits (DRR)-Total,Android, iOS,mWebPWA,Platform bounce-Total ,Android, iOS,mWeb,PWA,Brand Visits (DRR)-Total Brands Croma,BB,CLiQ,1mg,Titan, Tanishq, IHCL,FS",\
    "Organic Traffic , Inorganic Traffic (Paid),% Organic Traffic",\
    "MAU-Total MAU (App only), New MAU (App only),Repeat MAU (App only), Repeat MAU%(on installed base),Visit Frequency",\
    "%Traffic initiating search,Search listing CTR (%)",\
    "%Brand visits initiated by search",\
    "Null Search,Requests,Null Search Ratio",\
    "%Page views wth OOS(%)-Croma",\
    "Avg transition time to brand on 4G: Android <= 4GB",\
    "uygyhby",\
    "PDP Visits /Brand visits,PDP to cart Conversion,Cart to Checkout Conversion,Checkout to Transaction Conversion",\
    "OPEL",\
    "TPM",\
    "Total Transactions",\
    "Transactors (ex FS)(Weekly KPIs)-Total DRR,New,Repeat,Croma,CLiQ,BB,1mg,Westside,Titan,Tanishq,IHCL,AirAsia.",\
    "MAC / MAU %,Transaction Frequency,Multicategory Transactor",\
    "Retention",\
    "GMV",\
    "Neucoins Expired : DRR,NC Balance,Unique Redeemers on TN: DRR-On TN Total,TN Redeemers as % TN transactors.Unique Redeemers on Tata Ecosystem: DRR",\
    "Unique Redeemers on TN: DRR-Croma,CLiQ,BB,1mg,AirAsia",\
    "GOCR",\
    "FTR",\
    "Non - FTR resolution Time",\
    "# of tickets open > 3 days(Weekly KPIS)",\
    "CS NPS"]
total_metadata = ""
for num,(i,j) in  enumerate(zip(tables_list, tables_metadata)):
    total_metadata += f'Table name -  {i} Metadata - {j}, '

import re
df_names = []
for table in tables_list:
    name = re.findall("(?<=\.).*", table)
    df_names.append(name[0])

df_names2 = []
for table in tables_list:
    name = re.findall("(?<=\.).*", table)
    df_names2.append(name[0])


import re
df_list2 = []
for table in df_names2:
#   name = re.findall("(?<=\.).*", table)
#   table_names_metadata += ", " + (re.findall("(?<=\.).*", table)[0])
#   table_names_metadata2 += ", " + f"{table}"
    df_str2= f"{table} = pd.read_sql_query(f'SELECT * FROM {table}', con)"
# table_names_metadata.append(name)
#   df_list.append(df_str)
    df_list2.append(df_str2)
print("Step 2 successful")

import re
all_df = []
for table in tables_list:
    try:
        name = re.findall("(?<=\.).*", table)
        all_df.append(eval(name[0]))
    except:
        continue
print("Step 3 successful!")
df_names = []
for table in tables_list:
    try:
        name = re.findall("(?<=\.).*", table)
    #     eval(name[0])
        df_names.append(table)
    except:
        continue
print("Step 4 Successful!")
df_names2 = []
for table in tables_list:
    name = re.findall("(?<=\.).*", table)
    df_names2.append(name[0])
print(df_names2)
print("Step 5 Successful!")

con = postgresql_connect()

for d in df_list2:
    try:
        exec(d)
    except:
        continue

metadata_II = []
for i in df_names2:
    try:
        l = [col for col in eval(i).columns.values]
        listToStr = ', '.join(l)
        s = i + " columns - " + listToStr + ". "
        metadata_II.append(s)
    except Exception as e:
        print(e)
        
print("Step 6 Successful!")
# from langchain.text_splitter import CharacterTextSplitter
from langchain.schema.document import Document
from langchain.embeddings.openai import OpenAIEmbeddings
# from langchain.text_splitter import CharacterTextSplitter
from langchain.schema.document import Document
from langchain.vectorstores import FAISS
def get_text_chunks_langchain(df):
    # text_splitter = CharacterTextSplitter(chunk_size=500, chunk_overlap=100)
    docs = [Document(page_content=x) for x in df]
    return docs
print(metadata_II)
docs = get_text_chunks_langchain(metadata_II)
from langchain.chat_models import AzureChatOpenAI
import os

os.environ["OPENAI_API_TYPE"]="azure"
os.environ["OPENAI_API_VERSION"]="2023-07-01-preview"
os.environ["OPENAI_API_BASE"]="https://tdl-aoai-southindia.openai.azure.com/"# Your Azure OpenAI resource endpoint
os.environ["OPENAI_API_KEY"]="b34db39a21bd4d6aa73f5a12e9f6fdf9" # Your Azure OpenAI resource key
os.environ["OPENAI_CHAT_MODEL"]="tdl-gpt35-turbo" # Use name of deployment

embeddings1=OpenAIEmbeddings(openai_api_type="azure",openai_api_base="https://tdl-chatbot.openai.azure.com/",openai_api_version="2023-05-15", deployment="embedding-ada",model="text-embedding-ada-002", openai_api_key="95a179d989cd4aeb84d36edb2fc8991a",chunk_size=1)
print("Step 7 Successful")

llm=AzureChatOpenAI(temperature=0.0,openai_api_base="https://tdl-aoai-southindia.openai.azure.com/",

                    openai_api_type="azure" ,model="tdl-gpt35-turbo",openai_api_version="2023-07-01-preview",

                    deployment_name="tdl-gpt35-turbo"

                    )

# from langchain.embeddings import HuggingFaceBgeEmbeddings
# from transformers import AutoTokenizer
# embedding = OpenAIEmbeddings()

import sys
print(docs)
print(embeddings1)
db = FAISS.from_documents(docs, embeddings1)
print("Done preprocessing!")


# preprocess = preprocessing()[1]
    
 
   

def openai_call_output(question, docs_number):
  llm = call_gpt()
  from langchain.prompts import PromptTemplate
  # question = "What are major topics for this class?"
#   db = preprocess
  docs = db.similarity_search(question,docs_number)
  len(docs)
  # Build prompt
  template = """?. 
  {context}
  Question: {question}
  Helpful Answer: """
  QA_CHAIN_PROMPT = PromptTemplate.from_template(template)

  from langchain.callbacks import get_openai_callback
  qa_chain = RetrievalQA.from_chain_type(
      llm,
      retriever=db.as_retriever(),
      return_source_documents=True,
      chain_type_kwargs={"prompt": QA_CHAIN_PROMPT}
  )
  with get_openai_callback() as cb:
    result = qa_chain({"query": question})
    # print(result["result"])
    # print(cb)
    
  
  return [result, cb]

def convert_sql_query_to_table(result):
    global df3
    global error
    global df_matches
    df3 = None
    import re
    con = postgresql_connect()
    # cursor_obj = con.cursor()
    sql_query = result["result"]
    print(sql_query)
    try:
        df3 = pd.read_sql_query(sql_query,con)
    # df2.display()
  # r = json.loads(r)
  # sql_query = r["choices"][0]["message"]["content"]
    except:
        matches = [m.group(1) for m in re.finditer("```([\w\W]*?)```", sql_query)]

        df_matches = []
        for m in matches:
            try:
                m2 = m.replace("sql", "")
            except:
                m2= sql_query
            df_matches.append(m2)
        error = []
        for df in df_matches:
            try:
                df3= pd.read_sql_query(df,con)
        #             df3 = df3.distinct()
        #             df3.display()
            except Exception as e:
                error.append(e)
    con.close()
    return [df3, error]

def convert_python_query_to_table(result):
    import re
    global df_matches
    python_query = result["result"]
    print(python_query)
    try:
        exec(python_query)
    # df.display()
    # r = json.loads(r)
    # sql_query = r["choices"][0]["message"]["content"]
    except:
        matches = [m.group(1) for m in re.finditer("```([\w\W]*?)```", python_query)]

        df_matches = []
        for m in matches:
            try:
                m2 = m.replace("python", "")
            except:
                m2= python_query
            df_matches.append(m2)
        error = []
        for df in df_matches:
            try:
                exec(df)
            # df_display3 = df_display3.distinct()
            # df_display3.display()
            except Exception as e:
                error.append(e)

    return [df_matches, error]

def token_numbers_used(result):
  string = str(result)
  sub1 = "Tokens Used: "
  sub2 = "\n"
  
  # getting index of substrings
  idx1 = string.index(sub1)
  idx2 = string.index(sub2)
  
  res = ''
  # getting elements in between
  for idx in range(idx1 + len(sub1) , idx2):
      res = res + string[idx]
  return int(res)

def cost_of_tokens(result):
    string = str(result)
    sub1 = "Total Cost (USD): $"
    sub2 = string[:-1]

    # getting index of substrings
    idx1 = string.index(sub1)
    idx2 = len(string)

    res = ''
    # getting elements in between
    for idx in range(idx1 + len(sub1), idx2):
      res = res + string[idx]
    return float(res)

def metadata_dict():
    metadata_dic = {}
    # df_names2 = preprocessing()[0]
    for df in df_names2 :
        try:
            metadata_dic[df] = list(eval(df).columns)
        except:
            continue
    return metadata_dic

def tables_columns_output(q):
    print(q)
    metadata_dic = metadata_dict()
    # metadata_II = preprocessing()[2]
    # db = preprocessing()[1]
    llm = call_gpt()
    from langchain.prompts import PromptTemplate
    question = f'''Given tables and column names given in the dictionary {metadata_dic}, and metadata for each table as 
    {metadata_II},find the relevant tables and column names to answer the question - '''+q + '''Give output in natural language and as a 
    dictionary with keys as required table names and values as column names in a list and enclose the dictionary in 
    triple backquote. Use joins only where necessary.If there are no common columns to join on, 
    do not consider the table.Consider a maximum of 5 most relevant tables. Consider only tables which 
     are in the date range of other tables join is over date. Do not consider tables which have few rows 
     compared to other tables and tables which have fewer than 100 rows.'''
    # question = "What are major topics for this class?"
    docs = db.similarity_search(question,5)

    len(docs)
    # Build prompt
    template = """?.  
    {context}
    Question: {question}
    Helpful Answer: """
    QA_CHAIN_PROMPT = PromptTemplate.from_template(template)

    from langchain.callbacks import get_openai_callback
    qa_chain = RetrievalQA.from_chain_type(
      llm,
      retriever=db.as_retriever(),
      return_source_documents=True,
      chain_type_kwargs={"prompt": QA_CHAIN_PROMPT}
    )
    with get_openai_callback() as cb:
        result = qa_chain({"query": question})
        print(result["result"])
        instructions2 = result["result"]
    return instructions2

def unique_vals_dict():
    # df_names2 = preprocessing()[0]
    unique_vals_dic = {}
    for t1 in df_names2:
        unique_vals = []
        try:
            t3 = eval(t1)
            for col in t3.columns:
                try:
                    u = np.unique(t3[col])
                    if len(u) < 20:
                        unique_vals.append([col,u ])
                    unique_vals_dic[t1] = unique_vals
                except:
                    pass 
        except:
            pass

def error_func(e,q):
    llm = call_gpt()
    # db = preprocessing()[1]
    error_dict = {
            "date_error": "There is some problem with the dates being considered or date column doesnt exist",
            "column_error": '''There is some problem with the columns being considered, either they dont exist or 
            the column name is incorrect.'''
            }
    from langchain.prompts import PromptTemplate
    instruction = f'''Given error in output to the question in{error_dict}, identify from the error{e} what type of error it is with key as
    type of error and value as the modified statement to deal with the error from, also identify from error {e} what column the error is on if it
     is a column error and create a new dictionary with only 1 key as the required 
    timeframe in error_dict.keys() and 1 value as a simplified version of the problem in natural language 
    and include the errorneous column name if it is column error and the date column and date if it is date error. 
    Store the result in a dictionary in triple backquotes.'''
    # question = "What are major topics for this class?"
    question = q + instruction 
    docs = db.similarity_search(question,5)

    len(docs)
    # Build prompt
    template = """?.  
    {context}
    Question: {question}
    Helpful Answer: """
    QA_CHAIN_PROMPT = PromptTemplate.from_template(template)

    from langchain.callbacks import get_openai_callback
    qa_chain = RetrievalQA.from_chain_type(
      llm,
      retriever=db.as_retriever(),
      return_source_documents=True,
      chain_type_kwargs={"prompt": QA_CHAIN_PROMPT}
    )
    with get_openai_callback() as cb:
        result = qa_chain({"query": question})
        print(result["result"])
        instructions = result["result"]
    result["chat_history"] = []
    return instructions

def genAIOutput(q):
    # preprocessing()
    # print(marketing_kpi_gold_monthly)
    import re
    con = postgresql_connect()
    import time
    metadata_dic = metadata_dict()
    unique_vals_dic = unique_vals_dict()
    print("Success 1!")
    time1 = time.time()
    matches = [m.group(1) for m in re.finditer("```([\w\W]*?)```", tables_columns_output(q))]
    matches[0] = matches[0].replace("python", "")
    #         m2 = m2.replace("\\n", " ")
    #         m2 = m2.replace("\\u003c", "")
    #         m2 = m2.replace("\\u003e", "")
    print(matches[0], "Success 2!")
    dic_final = eval(matches[0])

    # dic_final3 = {}
    # for k in dic_final.keys():
    #     if len(eval(k)) >= 100:
    #         dic_final3[k] = dic_final[k]
    print("Step 1 genAI!")
    dic_final2 = {} 
    for key1 in dic_final.keys():
        l = []
        for v1 in dic_final[key1]:
            if v1 in metadata_dic[key1]:
                l.append(v1)
                dic_final2[key1] = l
                

    print("Step2 genAI!")     
    time2 = time.time()
    print("Getting tables time:",time2-time1)
    instructions = f'''You are a data scientist. The given question is a qualitative question which requires an answer
    based on some insights into the given data and some statistical analyses on the same to give a better understndin
    to the person asking the question. It should consider the question from different angles and provide insights based
    on these different perspectives. Given the dictionary of required table and columns as dictionary dic_final2 {dic_final2} where keys are required tables 
    and columns as required columns and unique_vals_dic {unique_vals_dic} is the dictionary with table name as key and column names
    with unique values in column less than 20 as the values of the dictionary to answer the given query, 
    give a syntactically correct spark SQL query running on databricks platform enclosed in triple backquote after 
    opening statement. Use only the given tables and column names in dic_final2{dic_final2} to construct the SQL
    query. '''
        
    instructions1 = instructions + '''Note that in the table daily_visit_conversion_rate_fy24, in column brand, 
    BB_Total is Big Basket or BB and Electronics_Total is Croma.'''

    question = q + instructions1
    tokens = 0
    token_cost = 0
    openai_output = openai_call_output(question,5)
    # print(openai_output[0]["result"])
    result = openai_output[0]
    tokens += token_numbers_used(openai_output[1])
    token_cost += cost_of_tokens(openai_output[1])
    global df3
    error = []
    #     conn_to_postgresql()
    df_display3 = convert_sql_query_to_table(result)[0]
    error = convert_sql_query_to_table(result)[1]
    if not error and df_display3 is None:
        error.append("The given dataframe is NoneType. Please revise the query")
    elif not error and len(df_display3) < 1:
        error = "The dataframe has no rows. Please give a revised query based on the same or choose another table from the existing tables."
    elif not error and len(df_display3) == 1 and (df_display3.iloc[0,0] == 0 or df_display3.iloc[0,0] == None):
        error = "This is not a satisfactory answer. Please refine the output or look into alternate table to get the output."

    total_error = ""
    num1 = 0
    while error != [] and num1 <= 2:
        total_error = error_func(error,q)
        print(total_error)
        error_instructions = f'''Given error {total_error} on first few passes through the model, give a revised query which is different
        from the previous query dealing with the error total_error which was encountered.'''
        question = q + "" + error_instructions + instructions1
        openai_output = openai_call_output(question,5)
        result = openai_output[0]
        print(result["result"])
        tokens += token_numbers_used(openai_output[1])
        token_cost += cost_of_tokens(openai_output[1])
        df_display3 = convert_sql_query_to_table(result)[0]
        error = convert_sql_query_to_table(result)[1]
        if not error and df_display3 is None:
            error.append("The given dataframe is NoneType. Please revise the query")
        elif not error and len(df_display3) < 1:
            error.append("The dataframe has no rows. Please give a revised query based on the same or choose another table from the existing tables.")
        elif not error and len(df_display3) == 1 and (df_display3.iloc[0,0] == 0 or df_display3.iloc[0,0] == None):
            error = "This is not a satisfactory answer. Please refine the output or look into alternate table to get the output."
        num1 += 1
        print(error)

    print("Finished step 3")

    time3 = time.time()
    print("Finished step3 time:", time3 - time2)

    df_display = df_display3

    print("Finished step 4")
    rows = []
    print(df_display)
    if df_display is None:
        nl_summary = "Sorry, no output possible, please try again or report the same.Thank you!"
    else:
        for r in df_display.items():
            rows.append(r)
        time4 = time.time()
        print("Appending all rows time:", time4 - time3)
        nl_instructions = f'''Given table df_display {df_display} and rows of this dataframe as {rows}, 
        give a natural language summary and detailed insights of the table which answers the question {q} q in as detailed manner as 
        possible of not less than 300 words in bullet point format highligting the important points...''' 
        question = q + nl_instructions
        openai_output = openai_call_output(question,5)
        result = openai_output[0]
        # print(result["result"])
        tokens += token_numbers_used(openai_output[1])
        token_cost += cost_of_tokens(openai_output[1])
        # print(result["result"])
        nl_summary = result["result"]
        time5 = time.time()
        print("NL output time:", time5-time4)
        nl_instructions = f'''Given table df_display {df_display} and columns as {df_display.columns}  
        give the result of the question {q} q  as a suitable graph, chart or histogram in python syntax. 
        Provide at least 3 different graphs to illustrate the same...'''
        question = q + nl_instructions
        openai_output = openai_call_output(question,5)
        result = openai_output[0]
        # print(result["result"])
        tokens += token_numbers_used(openai_output[1])
        token_cost += cost_of_tokens(openai_output[1])

        error3 = []
        try:
            df_matches = convert_python_query_to_table(result)[0]
        except:
            error3 = convert_python_query_to_table(result)[1]
        total_error3 = ""
        num3 = 0
        while error3 != [] and num3 <= 2:
            num3 += 1
            total_error3 += str(error3[0]) + " "
            error_instructions = f"Given error {total_error3} on first few passes through the model, give a revised output for the query."
            question = q + " "+ error_instructions + " " + nl_instructions
            openai_output = openai_call_output(question,5)
            result = openai_output[0]
        #     print(result["result"])
            tokens += token_numbers_used(openai_output[1])
            token_cost += cost_of_tokens(openai_output[1])
            df_matches = convert_python_query_to_table(result)[0]
            error3 = convert_python_query_to_table(result)[1]
            print(error3)
        
       
        s = ("The token count for the enture query is: ", tokens)
        s2 = ("The cost for the entire query is: ", token_cost)
        time6 = time.time()
        print("Graph output time: ", time6 - time5)
        print(nl_summary, s, s2)
    time_final = time.time()
    total_time = time_final - time1
    print(total_time)
    try:
        if len(df_matches) > 1:
            for df in df_matches:
                exec(df_matches)
        else:
            exec(df_matches)
    except:
        pass
    con.close()
    return [nl_summary,s,s2]


