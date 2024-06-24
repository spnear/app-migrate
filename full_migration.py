import os
from dotenv import load_dotenv
import io
import boto3
import pandas as pd

load_dotenv("./s3.env")

ACCESS_KEY = os.getenv('ACEESS_KEY')
SECRET_ACCESS_KEY = os.getenv('SECRET_ACCESS_KEY')

s3 = boto3.client(
    's3',
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_ACCESS_KEY,
)

s3_file_key = 'departments.csv'
bucket = 'raw-migration-sebastianpasar'


obj = s3.get_object(Bucket=bucket, Key=s3_file_key)
initial_df = pd.read_csv(io.BytesIO(obj['Body'].read()))

print(initial_df.head())