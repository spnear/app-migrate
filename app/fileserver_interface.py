import os
from dotenv import load_dotenv
from datetime import datetime, timezone

load_dotenv()

class FileServerInterface():
    def __init__(self) -> None:
        self.PATH_NULLS = os.environ["NULL_PATH"]

    def write_to_fileserver(self,df=None,table:str=None):
        execution_time = datetime.now(timezone.utc).strftime("%Y%m%d_%H:%M:%S")
        file_path = os.path.join(self.PATH_NULLS,f'{execution_time}_{table}.csv')
        if len(df)> 0:
            df.to_csv(file_path,index=False) 
            print(file_path,'saved')

