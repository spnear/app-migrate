import os
from dotenv import load_dotenv
import io
import boto3
import pandas as pd
from models import TableSchema
import pandavro as pdx

load_dotenv()

ACCESS_KEY = os.getenv('ACEESS_KEY')
SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY,
)


class S3Interface(TableSchema):

    def __init__(self) -> None:
        self.ACCESS_KEY = os.getenv('ACEESS_KEY')
        self.SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')

    def _get_schema(self,table):
        table_schemas = {
            'departments': self.department_schema.keys(),
            'hired_employees': self.hired_employees_schema.keys(),
            'jobs': self.jobs_schema.keys()
        }
        return list(table_schemas[table])

    def read_csv_from_s3(self, bucket, table):
        schema=self._get_schema(table)
        obj = s3.get_object(Bucket=bucket, Key=f'{table}.csv')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()),header=None, names=schema)
        return df

    def write_csv_to_s3(self, bucket, df):
        csv_buffer=io.StringIO()
        df.to_csv(csv_buffer)
        content = csv_buffer.getvalue()
        s3.put_object(Bucket=bucket, Body=content,Key='testing.csv')

    def write_avro_to_s3(self, bucket, df, table):
        avro_buffer=io.BytesIO()
        pdx.to_avro(avro_buffer,df)
        content = avro_buffer.getvalue()
        s3.put_object(Bucket=bucket, Body=content,Key=f'{table}.avro')

