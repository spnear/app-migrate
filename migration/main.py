from sqlalchemy import create_engine, Table, Column, Integer,Float, String, MetaData 
import pandas as pd
import os
from dotenv import load_dotenv

from datetime import datetime, timezone

load_dotenv()

HOST=os.getenv('DATABASE_HOST')
PORT=os.getenv('DATABASE_PORT')
DATABASE=os.getenv('DATABASE_NAME')
USERNAME=os.getenv('DATABASE_USERNAME')
PASSWORD=os.getenv('DATABASE_PASSWORD')

SQLALCHEMY_DATABASE_URL = f'postgresql://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}'
print(SQLALCHEMY_DATABASE_URL)

metadata = MetaData() 
engine = create_engine(SQLALCHEMY_DATABASE_URL) 

df_departments = pd.read_csv('raw/departments.csv',header=None, names=['id', 'department']) 
df_departments.to_sql('departments', con=engine, if_exists='append', index=False) 

df_jobs = pd.read_csv('raw/jobs.csv',header=None, names=['id', 'job']) 
df_jobs.to_sql(name='jobs', con=engine, if_exists = 'replace',index=False)

df_hired_employees = pd.read_csv('raw/hired_employees.csv',header=None, names=['id', 'name', 'datetime','department_id','job_id']) 
df_hired_employees['department_id'] = df_hired_employees['department_id'].astype('Int64')
df_hired_employees['job_id'] = df_hired_employees['job_id'].astype('Int64')
df_nulls = df_hired_employees.isna().any(axis=1)
df_nulls = df_hired_employees[df_nulls]

df_hired_employees_to_load = df_hired_employees.dropna()
df_hired_employees_to_load.to_sql(name='hired_employees', con=engine, if_exists = 'replace',index=False)

execution_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")
df_nulls.to_csv(f'raw/nulls/{execution_time}_hired_employees.csv',header=None,index=None)

