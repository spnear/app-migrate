import re
import json
import pandas as pd
import numpy as np
from models import TableSchema


class CommonFunctions():
    
    def read_df_from_json(self,str_json):
        if type(str_json) == bytes:
            bstr_json = str_json
        else:
            bstr_json = bytes(str_json,'utf-8')
        json_object = json.loads(bstr_json)
        df = pd.DataFrame(json_object)
        if len(df)>0:
            return df
        else:
            return None
        
    def df_to_json(self, df):
        if len(df)>0:
            return df.to_json(index=False,orient='records')
        else:
            print('Query did not retrieve data')
    

class ProcessingData(TableSchema):

    def __init__(self,df,table) -> None:
        self.df = df
        self.table = table

    def str_to_int(self,str_number:str) -> int:
        str_number = str(str_number)
        str_number = str_number.split('.')[0]
        if str_number.isnumeric():
            int_number = int(str_number)
        else:
            int_number = 'NA'
        return int_number
    
    def execute_processing(self):
        if self.table == 'departments':
              schema = self.department_schema
              df_to_load,df_nulls = self._processing_departments_info(self.df, schema)
        elif self.table == 'hired_employees':
              schema = self.hired_employees_schema
              df_to_load,df_nulls = self._processing_departments_info(self.df, schema)
        elif self.table == 'jobs':
              schema = self.jobs_schema
              df_to_load,df_nulls = self._processing_departments_info(self.df, schema)
        else:
              df_to_load,df_nulls = None,None
        return df_to_load, df_nulls

    def _processing_hiring_info(self,df,schema=None):
        df = df.apply(lambda x: x.str.strip() if isinstance(x, str) else x).replace('', np.nan)
        nulls = df.isna().any(axis=1)
        df_nulls = df[nulls]
        df = df.dropna()
        
        df['department_id'] = df['department_id'].apply(self.str_to_int)
        df['job_id'] = df['job_id'].apply(self.str_to_int)
        df = df[(df['department_id']!='NA') & (df['job_id']!='NA')]


        for col in schema.keys():
            df[col] = df[col].astype(schema[col])

        
        return df,df_nulls
    
    def _processing_departments_info(self,df,schema):
        df = df.apply(lambda x: x.str.strip() if isinstance(x, str) else x).replace('', np.nan)
        nulls = df.isna().any(axis=1)
        df_nulls = df[nulls]
        df = df.dropna()

        for col in schema.keys():
            df[col] = df[col].astype(schema[col])

        return df,df_nulls
    
    def _processing_jobs_info(self,df,schema):
        df = df.apply(lambda x: x.str.strip() if isinstance(x, str) else x).replace('', np.nan)
        nulls = df.isna().any(axis=1)
        df_nulls = df[nulls]
        df = df.dropna()

        for col in schema.keys():
            df[col] = df[col].astype(schema[col])

        return df,df_nulls
    

    



