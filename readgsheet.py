import gspread
from oauth2client.service_account import ServiceAccountCredentials as sac
import pandas as pd
import sqlalchemy as sa
# from sqlalchemy import Table, Column, String, select, Integer
# import os
# from dotenv import load_dotenv
# import psycopg2


# function to retrieve data from the g spreedsheet

def gsheet2df(spreadsheet_name, sheet_num):
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    credentials_path = 'project-368812-542f0f0ad44b.json'
    
    credentials = sac.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(credentials)

    sheet = client.open(spreadsheet_name).get_worksheet(sheet_num).get_all_records()
    print(sheet)
    df =  pd.DataFrame.from_dict(sheet)
    
    return df

data = gsheet2df('insurance', 0)


# establishing connection to postgresql database

POSTGRES_USER="root"
POSTGRES_PASSWORD="password"
POSTGRES_DB= "postgres"
POSTGRES_HOST= "localhost"
POSTGRES_PORT= 5432

# ### CREATE A CONNECTION TO REDSHIFT DB
connection_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (POSTGRES_USER,POSTGRES_PASSWORD,POSTGRES_HOST,str(POSTGRES_PORT),POSTGRES_DB)
engine = sa.create_engine(connection_string, pool_pre_ping=True)
connection = engine.connect()
print(connection)


# #outputs the execution time of the python statement 
# Insurance_table = Table('data', connection,
#                             Column('age', String),
#                             Column('sex', String),
#                             Column('bmi', String),
#                             Column('children', String),
#                             Column('smoker', String),
#                             Column('region', String),
#                             Column('charges', Integer)
#                             )
data.to_sql('Insurance', con = engine, if_exists = 'replace', index= False, schema = "public")
# connection.execute('COMMIT')
# connection.execute('grant select on public.Insurance to root;')
# connection.execute('COMMIT')
Query = """
        SELECT * FROM "Insurance"
        
        """
result = connection.execute(Query)

for row in result:
    print(row)
        
