import pandas as pd
import os
from dotenv import load_dotenv
from connections import Connection
import pandavro as pdx
from aws_interface import S3Interface
load_dotenv()

class DatabaseInterface(Connection):
    def __init__(self) -> None:
        self.BACKUP_PATH = os.environ["BACKUP_PATH"]

    def write_to_database(self,df,table):
        try:
            engine = self.engine()
            df.to_sql(name=table, con=engine, if_exists = 'append',index=False)
            return True
        except:
            return False
    
    def read_from_database(self, query):
        engine = self.engine()
        df = pd.read_sql(query,con=engine)
        return df

    def generate_table_backup(self,table):
        engine = self.engine()
        df = pd.read_sql(f'select * from {table}',con=engine)
        table_path = os.path.join(self.BACKUP_PATH,table)
        pdx.to_avro(table_path,df)
        return f'backup for {table} was generated on {table_path}'

    def generate_table_backup_s3(self,table):
        engine = self.engine()
        df = pd.read_sql(f'select * from {table}',con=engine)
        s3_loader = S3Interface()
        bucket=os.getenv('BUCKET_BACKUP')
        s3_loader.write_avro_to_s3(bucket, df, table)
        return f'backup for {table} was generated on f{bucket}/{table}'

    def restore_table_backup(self,table):
        engine = self.engine()
        table_path = os.path.join(self.BACKUP_PATH,table)
        df = pdx.read_avro(table_path)
        df.to_sql(name=table, con=engine, if_exists = 'replace',index=False)
        return f'{table} restauration from backup was successfull'